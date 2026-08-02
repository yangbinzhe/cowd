use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

use futures::StreamExt;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    events::CowdEventSender,
    protocol::{
        GatewayEventCorrelation, GatewaySessionEvent, SessionMessagesPage,
        SessionStreamConnectionState,
    },
    CowdEvent,
};
use cowd_app_host::TuiAppEvent;

const GATEWAY_READY_RETRY_ATTEMPTS: usize = 20;
const GATEWAY_READY_RETRY_DELAY: Duration = Duration::from_millis(100);
const DEFAULT_GATEWAY_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_GATEWAY_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const GATEWAY_SSE_IDLE_TIMEOUT: Duration = Duration::from_secs(45);
const DEFAULT_GATEWAY_URL: &str = "http://127.0.0.1:8642";
const TUI_SURFACE_ID: &str = "tui";

fn authorize_tui_request(
    request: reqwest::RequestBuilder,
    auth_token: Option<&str>,
    observer_id: &str,
) -> reqwest::RequestBuilder {
    let request = request
        .header("x-cowd-surface-id", TUI_SURFACE_ID)
        .header("x-cowd-observer-id", observer_id);
    if let Some(token) = auth_token.filter(|token| !token.trim().is_empty()) {
        request.bearer_auth(token.trim())
    } else {
        request
    }
}

#[derive(Debug, Clone)]
pub struct GatewayApiClient {
    base_url: String,
    auth_token: Option<String>,
    observer_id: String,
    client: reqwest::Client,
    /// Long-lived streams cannot share the ordinary 15-second total request
    /// deadline. A per-read idle watchdog still detects missing heartbeats.
    sse_client: reqwest::Client,
    live: Arc<TuiLiveMultiplexer>,
}

#[derive(Debug)]
struct TuiLiveMultiplexer {
    transport: LiveTransportConfig,
    commands: Mutex<Option<mpsc::UnboundedSender<LiveCommand>>>,
}

#[derive(Debug, Clone)]
struct LiveTransportConfig {
    base_url: String,
    auth_token: Option<String>,
    observer_id: String,
    client: reqwest::Client,
    sse_client: reqwest::Client,
}

impl LiveTransportConfig {
    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        authorize_tui_request(request, self.auth_token.as_deref(), &self.observer_id)
    }
}

#[derive(Debug)]
enum LiveCommand {
    Add {
        subscriber_id: String,
        source: harness_contract::live::LiveSourceSelector,
        tx: mpsc::Sender<harness_contract::live::LiveEnvelope>,
        ack: oneshot::Sender<Result<(), String>>,
    },
    Remove {
        subscriber_id: String,
        source_key: String,
    },
}

struct TuiLiveLease {
    subscriber_id: String,
    source_key: String,
    commands: mpsc::UnboundedSender<LiveCommand>,
    rx: mpsc::Receiver<harness_contract::live::LiveEnvelope>,
}

impl TuiLiveLease {
    async fn recv(&mut self) -> Option<harness_contract::live::LiveEnvelope> {
        self.rx.recv().await
    }
}

impl Drop for TuiLiveLease {
    fn drop(&mut self) {
        let _ = self.commands.send(LiveCommand::Remove {
            subscriber_id: self.subscriber_id.clone(),
            source_key: self.source_key.clone(),
        });
    }
}

#[derive(Debug)]
enum LiveTransportEvent {
    Envelope(harness_contract::live::LiveEnvelope),
    Interrupted(String),
    Recreate(String),
}

#[derive(Debug)]
struct LiveSubscriber {
    selector: harness_contract::live::LiveSourceSelector,
    tx: mpsc::Sender<harness_contract::live::LiveEnvelope>,
}

#[derive(Debug)]
struct LiveSourceState {
    selector: harness_contract::live::LiveSourceSelector,
    subscribers: BTreeMap<String, LiveSubscriber>,
    pending_previews: BTreeMap<String, harness_contract::live::LiveEnvelope>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionStreamProgress {
    pub commit_cursor: Option<u64>,
    pub next_message_sequence: usize,
}

/// Sanitized failure delivered to an external APP terminal panel. Credentials
/// and host implementation details never cross this boundary.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AppTransportFailure {
    pub status: Option<u16>,
    pub body: Option<serde_json::Value>,
    pub message: String,
}

#[derive(Debug)]
pub enum GatewayApiError {
    Http(reqwest::Error),
    Status(reqwest::StatusCode, String),
    /// The session SSE body already emitted and delivered the typed terminal
    /// revoke event. The runner must stop without synthesizing a duplicate.
    SessionAuthorizationRevoked(String),
    Contract(String),
    Url(String),
}

impl TuiLiveMultiplexer {
    async fn subscribe(
        &self,
        source: harness_contract::live::LiveSourceSelector,
    ) -> Result<TuiLiveLease, GatewayApiError> {
        let commands = {
            let mut slot = self
                .commands
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(commands) = slot.as_ref() {
                commands.clone()
            } else {
                let (commands, receiver) = mpsc::unbounded_channel();
                tokio::spawn(run_tui_live_manager(self.transport.clone(), receiver));
                *slot = Some(commands.clone());
                commands
            }
        };
        let subscriber_id = uuid::Uuid::new_v4().to_string();
        let source_key = source.key();
        let (tx, rx) = mpsc::channel(256);
        let (ack_tx, ack_rx) = oneshot::channel();
        commands
            .send(LiveCommand::Add {
                subscriber_id: subscriber_id.clone(),
                source,
                tx,
                ack: ack_tx,
            })
            .map_err(|_| GatewayApiError::Url("TUI live manager is unavailable".to_string()))?;
        ack_rx
            .await
            .map_err(|_| GatewayApiError::Url("TUI live manager stopped".to_string()))?
            .map_err(GatewayApiError::Contract)?;
        Ok(TuiLiveLease {
            subscriber_id,
            source_key,
            commands,
            rx,
        })
    }
}

async fn run_tui_live_manager(
    transport: LiveTransportConfig,
    mut commands: mpsc::UnboundedReceiver<LiveCommand>,
) {
    let (event_tx, mut event_rx) = mpsc::channel(512);
    let mut sources = BTreeMap::<String, LiveSourceState>::new();
    let mut subscription: Option<harness_contract::live::LiveSubscription> = None;
    let mut connection: Option<tokio::task::JoinHandle<()>> = None;
    let mut ready_revision = 0u64;
    let mut pending_revision_envelopes = Vec::<harness_contract::live::LiveEnvelope>::new();

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break; };
                match command {
                    LiveCommand::Add { subscriber_id, source, tx, ack } => {
                        let key = source.key();
                        let source_state = sources.entry(key.clone()).or_insert_with(|| LiveSourceState {
                            selector: source.clone(),
                            subscribers: BTreeMap::new(),
                            pending_previews: BTreeMap::new(),
                        });
                        source_state.subscribers.insert(
                            subscriber_id.clone(),
                            LiveSubscriber {
                                selector: source,
                                tx,
                            },
                        );
                        refresh_tui_live_source_selector(source_state);
                        let previous_revision =
                            subscription.as_ref().map_or(0, |active| active.revision);
                        let result = sync_tui_live_subscription(
                            &transport,
                            &sources,
                            &mut subscription,
                        ).await;
                        if result.is_err() {
                            let mut remove_source = false;
                            if let Some(source) = sources.get_mut(&key) {
                                source.subscribers.remove(&subscriber_id);
                                source.pending_previews.remove(&subscriber_id);
                                remove_source = source.subscribers.is_empty();
                                if !remove_source {
                                    refresh_tui_live_source_selector(source);
                                }
                            }
                            if remove_source {
                                sources.remove(&key);
                            }
                        }
                        let current_revision =
                            subscription.as_ref().map_or(0, |active| active.revision);
                        if result.is_ok() && previous_revision != current_revision {
                            if ready_revision != previous_revision {
                                ready_revision = 0;
                            }
                            pending_revision_envelopes.clear();
                        }
                        if result.is_ok() && connection.as_ref().is_none_or(|task| task.is_finished()) {
                            if let Some(active) = subscription.clone() {
                                if let Some(old) = connection.take() {
                                    old.abort();
                                }
                                connection = Some(tokio::spawn(run_tui_live_connection(
                                    transport.clone(),
                                    active,
                                    event_tx.clone(),
                                )));
                            }
                        }
                        let _ = ack.send(result);
                    }
                    LiveCommand::Remove { subscriber_id, source_key } => {
                        let mut changed = false;
                        let mut remove_source = false;
                        if let Some(source) = sources.get_mut(&source_key) {
                            changed = source.subscribers.remove(&subscriber_id).is_some();
                            source.pending_previews.remove(&subscriber_id);
                            remove_source = source.subscribers.is_empty();
                            if changed && !remove_source {
                                refresh_tui_live_source_selector(source);
                            }
                        }
                        if remove_source {
                            sources.remove(&source_key);
                        }
                        if changed {
                            let previous_revision =
                                subscription.as_ref().map_or(0, |active| active.revision);
                            let result = sync_tui_live_subscription(
                                &transport,
                                &sources,
                                &mut subscription,
                            ).await;
                            let current_revision =
                                subscription.as_ref().map_or(0, |active| active.revision);
                            if result.is_ok() && previous_revision != current_revision {
                                if ready_revision != previous_revision {
                                    ready_revision = 0;
                                }
                                pending_revision_envelopes.clear();
                            }
                            if let Err(reason) = result {
                                let _ = event_tx
                                    .send(LiveTransportEvent::Interrupted(format!(
                                        "TUI live selector update failed: {reason}"
                                    )))
                                    .await;
                            }
                        }
                        if sources.is_empty() {
                            if let Some(task) = connection.take() {
                                task.abort();
                            }
                        }
                    }
                }
            }
            event = event_rx.recv() => {
                let Some(event) = event else { break; };
                match event {
                    LiveTransportEvent::Envelope(envelope) => {
                        let current_revision =
                            subscription.as_ref().map_or(0, |active| active.revision);
                        if envelope.subscription_revision < current_revision
                            && envelope.subscription_revision != ready_revision
                        {
                            continue;
                        }
                        if envelope.subscription_revision > current_revision {
                            let _ = event_tx
                                .send(LiveTransportEvent::Recreate(
                                    "Gateway advanced beyond the acknowledged subscription revision"
                                        .to_string(),
                                ))
                                .await;
                            continue;
                        }
                        if matches!(
                            envelope.event.as_str(),
                            "subscription.ready" | "subscription.revision.changed"
                        ) {
                            ready_revision = envelope.subscription_revision;
                            let pending = std::mem::take(&mut pending_revision_envelopes);
                            for queued in pending {
                                if queued.subscription_revision == ready_revision {
                                    deliver_tui_live_envelope(&mut sources, queued).await;
                                }
                            }
                            continue;
                        }
                        let can_precede_barrier = matches!(
                            envelope.source_health,
                            harness_contract::live::SourceHealth::Baseline
                                | harness_contract::live::SourceHealth::Revoked
                                | harness_contract::live::SourceHealth::ResyncRequired
                        );
                        if ready_revision != envelope.subscription_revision
                            && !can_precede_barrier
                        {
                            if pending_revision_envelopes.len() >= 1_024 {
                                let _ = event_tx
                                    .send(LiveTransportEvent::Recreate(
                                        "TUI live revision barrier buffer exceeded its safety bound"
                                            .to_string(),
                                    ))
                                    .await;
                            } else {
                                pending_revision_envelopes.push(envelope);
                            }
                            continue;
                        }
                        deliver_tui_live_envelope(&mut sources, envelope).await;
                    }
                    LiveTransportEvent::Interrupted(reason) => {
                        ready_revision = 0;
                        pending_revision_envelopes.clear();
                        deliver_tui_live_resync(&mut sources, &subscription, &reason).await;
                    }
                    LiveTransportEvent::Recreate(reason) => {
                        if let Some(task) = connection.take() {
                            task.abort();
                        }
                        deliver_tui_live_resync(&mut sources, &subscription, &reason).await;
                        if let Some(active) = subscription.take() {
                            let _ = tui_live_delete(&transport, &active.id).await;
                        }
                        ready_revision = 0;
                        pending_revision_envelopes.clear();
                        if !sources.is_empty()
                            && sync_tui_live_subscription(
                                &transport,
                                &sources,
                                &mut subscription,
                            ).await.is_ok()
                        {
                            if let Some(active) = subscription.clone() {
                                connection = Some(tokio::spawn(run_tui_live_connection(
                                    transport.clone(),
                                    active,
                                    event_tx.clone(),
                                )));
                            }
                        }
                    }
                }
            }
        }
    }
    if let Some(task) = connection {
        task.abort();
    }
    if let Some(active) = subscription {
        let _ = tui_live_delete(&transport, &active.id).await;
    }
}

fn refresh_tui_live_source_selector(source: &mut LiveSourceState) {
    let Some(first) = source.subscribers.values().next() else {
        return;
    };
    let mut selector = first.selector.clone();
    selector.cursor = selector.cursor.max(source.selector.cursor);
    selector.revision = selector.revision.max(source.selector.revision);
    for subscriber in source.subscribers.values().skip(1) {
        selector.cursor = selector.cursor.max(subscriber.selector.cursor);
        selector.revision = selector.revision.max(subscriber.selector.revision);
        if subscriber.selector.detail_scope
            == harness_contract::projection::ProjectionDetailScope::Full
        {
            selector.detail_scope = harness_contract::projection::ProjectionDetailScope::Full;
        }
    }
    source.selector = selector;
}

async fn deliver_tui_live_resync(
    sources: &mut BTreeMap<String, LiveSourceState>,
    subscription: &Option<harness_contract::live::LiveSubscription>,
    reason: &str,
) {
    let subscription_id = subscription.as_ref().map_or_else(
        || "tui-live-unavailable".to_string(),
        |active| active.id.clone(),
    );
    let subscription_revision = subscription.as_ref().map_or(0, |active| active.revision);
    let envelopes = sources
        .values()
        .map(|source| {
            let kind = match source.selector.kind {
                harness_contract::live::LiveSourceKind::Session => "session",
                harness_contract::live::LiveSourceKind::Execution => "execution",
                harness_contract::live::LiveSourceKind::Mission => "mission",
            };
            harness_contract::live::LiveEnvelope {
                schema_version: harness_contract::live::LIVE_CONTRACT_SCHEMA_VERSION,
                subscription_id: subscription_id.clone(),
                subscription_revision,
                source_kind: kind.to_string(),
                source_id: source.selector.id.clone(),
                detail_scope: source.selector.detail_scope,
                source_cursor: Some(source.selector.cursor),
                delivery_class: harness_contract::live::DeliveryClass::Durable,
                source_health: harness_contract::live::SourceHealth::ResyncRequired,
                event: "source.resync_required".to_string(),
                payload: serde_json::json!({
                    "reason": reason,
                    "origin": "tui_live_transport",
                }),
                session_id: (kind == "session").then(|| source.selector.id.clone()),
                execution_id: (kind == "execution").then(|| source.selector.id.clone()),
                mission_id: (kind == "mission").then(|| source.selector.id.clone()),
                agent_id: None,
                stream_revision: None,
                start_bytes: None,
                end_bytes: None,
            }
        })
        .collect::<Vec<_>>();
    for envelope in envelopes {
        deliver_tui_live_envelope(sources, envelope).await;
    }
}

async fn deliver_tui_live_envelope(
    sources: &mut BTreeMap<String, LiveSourceState>,
    envelope: harness_contract::live::LiveEnvelope,
) {
    let key = format!("{}:{}", envelope.source_kind, envelope.source_id);
    let Some(source) = sources.get_mut(&key) else {
        return;
    };
    if let Some(cursor) = envelope.source_cursor {
        source.selector.cursor = source.selector.cursor.max(cursor);
    }
    if envelope.source_kind == "execution" {
        let revision = match envelope.event.as_str() {
            "projection_snapshot" => envelope
                .payload
                .get("revision")
                .and_then(serde_json::Value::as_u64),
            "projection_delta" => envelope
                .payload
                .get("target_revision")
                .and_then(serde_json::Value::as_u64),
            _ => None,
        };
        if let Some(revision) = revision {
            source.selector.revision = source.selector.revision.max(revision);
        }
    }
    let mut closed = Vec::new();
    for (subscriber_id, subscriber) in &source.subscribers {
        crate::performance::observe_count(
            "tui_live_queue_depth",
            subscriber
                .tx
                .max_capacity()
                .saturating_sub(subscriber.tx.capacity()),
        );
        let delivered = if envelope.delivery_class
            != harness_contract::live::DeliveryClass::EphemeralPreview
        {
            source.pending_previews.remove(subscriber_id);
            let started = Instant::now();
            let delivered = subscriber.tx.send(envelope.clone()).await.is_ok();
            crate::performance::observe_duration(
                "tui_reliable_delivery_wait_ms",
                started.elapsed(),
            );
            delivered
        } else {
            if let Some(pending) = source.pending_previews.remove(subscriber_id) {
                if let Err(mpsc::error::TrySendError::Closed(_)) = subscriber.tx.try_send(pending) {
                    closed.push(subscriber_id.clone());
                    continue;
                }
            }
            match subscriber.tx.try_send(envelope.clone()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(pending)) => {
                    source
                        .pending_previews
                        .insert(subscriber_id.clone(), pending);
                    crate::performance::observe_count("tui_preview_coalesced_count", 1);
                    true
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        };
        if !delivered && subscriber.tx.is_closed() {
            closed.push(subscriber_id.clone());
        }
    }
    for subscriber_id in closed {
        source.subscribers.remove(&subscriber_id);
        source.pending_previews.remove(&subscriber_id);
    }
}

async fn sync_tui_live_subscription(
    transport: &LiveTransportConfig,
    sources: &BTreeMap<String, LiveSourceState>,
    subscription: &mut Option<harness_contract::live::LiveSubscription>,
) -> Result<(), String> {
    let selector = harness_contract::live::LiveSelector {
        sources: sources
            .values()
            .map(|source| source.selector.clone())
            .collect(),
    };
    if selector.sources.is_empty() {
        if let Some(active) = subscription.take() {
            tui_live_delete(transport, &active.id).await?;
        }
        return Ok(());
    }
    let response = if let Some(active) = subscription.as_ref() {
        match tui_live_request(
            transport,
            reqwest::Method::PATCH,
            &format!("/api/runtime/live-subscriptions/{}", active.id),
            serde_json::json!({
                "expected_revision": active.revision,
                "idempotency_key": format!(
                    "tui-live-patch:{}:{}",
                    active.id, active.revision
                ),
                "selector": selector,
            }),
        )
        .await
        {
            Ok(response) => response,
            Err(error) if live_subscription_requires_recreate(&error) => {
                if let Some(stale) = subscription.take() {
                    let _ = tui_live_delete(transport, &stale.id).await;
                }
                create_tui_live_subscription(transport, selector).await?
            }
            Err(error) => return Err(error),
        }
    } else {
        create_tui_live_subscription(transport, selector).await?
    };
    let parsed = serde_json::from_value(response)
        .map_err(|error| format!("Gateway live subscription contract is invalid: {error}"))?;
    *subscription = Some(parsed);
    Ok(())
}

async fn create_tui_live_subscription(
    transport: &LiveTransportConfig,
    selector: harness_contract::live::LiveSelector,
) -> Result<serde_json::Value, String> {
    tui_live_request(
        transport,
        reqwest::Method::POST,
        "/api/runtime/live-subscriptions",
        serde_json::json!({
            "surface_instance": transport.observer_id,
            "selector": selector,
            "idempotency_key": format!("tui-live:{}", transport.observer_id),
        }),
    )
    .await
}

fn live_subscription_requires_recreate(error: &str) -> bool {
    ["404 Not Found", "409 Conflict", "410 Gone"]
        .iter()
        .any(|status| error.contains(status))
}

async fn tui_live_request(
    transport: &LiveTransportConfig,
    method: reqwest::Method,
    path: &str,
    body: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let request = transport.authorize(
        transport
            .client
            .request(method, format!("{}{}", transport.base_url, path))
            .json(&body),
    );
    let response = request.send().await.map_err(|error| error.to_string())?;
    let status = response.status();
    let body = response.text().await.map_err(|error| error.to_string())?;
    if !status.is_success() {
        return Err(format!(
            "Gateway live subscription returned {status}: {body}"
        ));
    }
    serde_json::from_str(&body).map_err(|error| error.to_string())
}

async fn tui_live_delete(
    transport: &LiveTransportConfig,
    subscription_id: &str,
) -> Result<(), String> {
    let request = transport.authorize(transport.client.delete(format!(
        "{}/api/runtime/live-subscriptions/{}",
        transport.base_url,
        url_encode(subscription_id)
    )));
    let response = request.send().await.map_err(|error| error.to_string())?;
    if response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND {
        Ok(())
    } else {
        Err(format!(
            "Gateway live subscription delete returned {}",
            response.status()
        ))
    }
}

async fn run_tui_live_connection(
    transport: LiveTransportConfig,
    subscription: harness_contract::live::LiveSubscription,
    tx: mpsc::Sender<LiveTransportEvent>,
) {
    let mut checkpoint: Option<String> = None;
    let mut retry = Duration::from_millis(250);
    let mut interruption_reported = false;
    loop {
        let mut request = transport.authorize(
            transport
                .sse_client
                .get(format!("{}{}", transport.base_url, subscription.stream_url))
                .header("Accept", "text/event-stream"),
        );
        if let Some(checkpoint) = checkpoint.as_deref() {
            request = request.header("Last-Event-ID", checkpoint);
        }
        let response = match request.send().await {
            Ok(response) if response.status().is_success() => response,
            Ok(response)
                if matches!(
                    response.status(),
                    reqwest::StatusCode::NOT_FOUND
                        | reqwest::StatusCode::GONE
                        | reqwest::StatusCode::CONFLICT
                ) =>
            {
                let _ = tx
                    .send(LiveTransportEvent::Recreate(format!(
                        "Gateway live subscription became invalid ({})",
                        response.status()
                    )))
                    .await;
                return;
            }
            Ok(response) => {
                if !interruption_reported {
                    interruption_reported = tx
                        .send(LiveTransportEvent::Interrupted(format!(
                            "Gateway live stream returned {}",
                            response.status()
                        )))
                        .await
                        .is_ok();
                }
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(Duration::from_secs(5));
                continue;
            }
            Err(error) => {
                if !interruption_reported {
                    interruption_reported = tx
                        .send(LiveTransportEvent::Interrupted(format!(
                            "Gateway live stream is unreachable: {error}"
                        )))
                        .await
                        .is_ok();
                }
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(Duration::from_secs(5));
                continue;
            }
        };
        interruption_reported = false;
        retry = Duration::from_millis(250);
        let mut bytes = response.bytes_stream();
        let mut buffer = Vec::new();
        let mut recreate = false;
        while let Some(chunk) = bytes.next().await {
            let Ok(chunk) = chunk else {
                break;
            };
            buffer.extend_from_slice(&chunk);
            while let Ok(Some(frame)) = take_gateway_sse_frame(&mut buffer) {
                if let Some(id) = gateway_sse_frame_id(&frame) {
                    checkpoint = Some(id.to_string());
                }
                let Some(data) = gateway_sse_frame_data(&frame) else {
                    continue;
                };
                let Ok(envelope) =
                    serde_json::from_str::<harness_contract::live::LiveEnvelope>(&data)
                else {
                    recreate = true;
                    break;
                };
                if envelope.subscription_id != subscription.id {
                    recreate = true;
                    break;
                }
                if envelope.event.starts_with("subscription.")
                    && envelope.source_health
                        == harness_contract::live::SourceHealth::ResyncRequired
                {
                    recreate = true;
                }
                if tx
                    .send(LiveTransportEvent::Envelope(envelope))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            if recreate {
                break;
            }
        }
        if recreate {
            let _ = tx
                .send(LiveTransportEvent::Recreate(
                    "Gateway requested live subscription resync".to_string(),
                ))
                .await;
            return;
        }
        if !interruption_reported {
            interruption_reported = tx
                .send(LiveTransportEvent::Interrupted(
                    "Gateway live stream ended before a terminal event".to_string(),
                ))
                .await
                .is_ok();
        }
        tokio::time::sleep(retry).await;
        retry = (retry * 2).min(Duration::from_secs(5));
    }
}

impl GatewayApiClient {
    #[must_use]
    pub fn observer_id(&self) -> &str {
        &self.observer_id
    }

    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        authorize_tui_request(request, self.auth_token.as_deref(), &self.observer_id)
    }

    pub fn new(
        base_url: impl Into<String>,
        auth_token: Option<String>,
    ) -> Result<Self, GatewayApiError> {
        let base_url = normalize_base_url(base_url.into())?;
        let client = reqwest::Client::builder()
            .connect_timeout(DEFAULT_GATEWAY_CONNECT_TIMEOUT)
            .timeout(DEFAULT_GATEWAY_REQUEST_TIMEOUT)
            .build()
            .map_err(GatewayApiError::Http)?;
        let sse_client = reqwest::Client::builder()
            .connect_timeout(DEFAULT_GATEWAY_CONNECT_TIMEOUT)
            .read_timeout(GATEWAY_SSE_IDLE_TIMEOUT)
            .build()
            .map_err(GatewayApiError::Http)?;
        let observer_id = std::env::var("COWD_TUI_OBSERVER_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("tui:{}", uuid::Uuid::new_v4()));
        let live = Arc::new(TuiLiveMultiplexer {
            transport: LiveTransportConfig {
                base_url: base_url.clone(),
                auth_token: auth_token.clone(),
                observer_id: observer_id.clone(),
                client: client.clone(),
                sse_client: sse_client.clone(),
            },
            commands: Mutex::new(None),
        });
        Ok(Self {
            base_url,
            auth_token,
            observer_id,
            client,
            sse_client,
            live,
        })
    }

    /// Return the APP ids registered by the connected Gateway.  The server is
    /// the startup-policy authority; the TUI uses this only to filter already
    /// compiled contributions and never to load source code dynamically.
    pub async fn enabled_app_ids(&self) -> Result<BTreeSet<String>, GatewayApiError> {
        let value = self.get_json("/api/apps").await?;
        let items = value
            .as_array()
            .ok_or_else(|| GatewayApiError::Url("application catalogue must be an array".into()))?;
        Ok(items
            .iter()
            .filter_map(|item| item.pointer("/descriptor/id").and_then(|id| id.as_str()))
            .map(str::to_string)
            .collect())
    }

    pub fn from_running_gateway(
        auth_token: Option<String>,
    ) -> Result<Option<Self>, GatewayApiError> {
        let base_url =
            std::env::var("COWD_GATEWAY_URL").unwrap_or_else(|_| DEFAULT_GATEWAY_URL.to_string());
        if !gateway_listener_reachable(&base_url) {
            return Ok(None);
        }
        Self::new(base_url, auth_token.or_else(default_auth_token)).map(Some)
    }

    pub fn from_running_gateway_with_retry(
        auth_token: Option<String>,
    ) -> Result<Option<Self>, GatewayApiError> {
        let auth_token = auth_token.or_else(default_auth_token);
        for attempt in 0..GATEWAY_READY_RETRY_ATTEMPTS {
            if let Some(client) = Self::from_running_gateway(auth_token.clone())? {
                return Ok(Some(client));
            }
            if attempt + 1 < GATEWAY_READY_RETRY_ATTEMPTS {
                std::thread::sleep(GATEWAY_READY_RETRY_DELAY);
            }
        }
        Ok(None)
    }

    pub fn ensure_running_with_retry(
        auth_token: Option<String>,
    ) -> Result<Option<Self>, GatewayApiError> {
        if let Some(client) = Self::from_running_gateway_with_retry(auth_token.clone())? {
            return Ok(Some(client));
        }

        if !cfg!(test) && std::env::var("COWD_DISABLE_DAEMON_AUTOSTART").is_err() {
            let exe =
                std::env::current_exe().map_err(|error| GatewayApiError::Url(error.to_string()))?;
            std::process::Command::new(exe)
                .arg("gateway")
                .arg("run")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .map_err(|error| GatewayApiError::Url(error.to_string()))?;
        }

        Self::from_running_gateway_with_retry(auth_token)
    }

    pub async fn runtime_control_plane(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/control-plane").await
    }

    pub async fn status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/status").await
    }

    pub async fn gateway_manifest(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/webui/manifest").await
    }

    pub async fn slash_projection(
        &self,
        surface: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/slash?surface={}", url_encode(surface)))
            .await
    }

    pub async fn slash_resolve(
        &self,
        input: &str,
        surface: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/slash/resolve",
            serde_json::json!({
                "input": input,
                "surface": surface,
                "context": {},
            }),
        )
        .await
    }

    pub async fn slash_dispatch(
        &self,
        command: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/slash/dispatch",
            serde_json::json!({
                "command": command,
                "args": args,
            }),
        )
        .await
    }

    pub async fn runtime_snapshot(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/snapshot").await
    }

    pub async fn list_sessions(&self) -> Result<serde_json::Value, GatewayApiError> {
        let mut offset = 0usize;
        let mut sessions = Vec::new();
        loop {
            let page = self
                .get_json(&format!(
                    "/api/sessions?limit=200&offset={offset}&sort=updated_at&order=desc"
                ))
                .await?;
            let page_sessions = page
                .get("sessions")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            let total = page
                .get("total")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(page_sessions.len() as u64) as usize;
            let fetched = page_sessions.len();
            sessions.extend(page_sessions);
            offset = offset.saturating_add(fetched);
            if fetched == 0 || offset >= total {
                return Ok(serde_json::json!({
                    "sessions": sessions,
                    "total": total,
                    "offset": 0,
                    "limit": sessions.len(),
                    "sort": "updated_at",
                    "order": "desc",
                }));
            }
        }
    }

    pub async fn create_session(
        &self,
        model: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/sessions", serde_json::json!({ "model": model }))
            .await
    }

    pub async fn rename_session(
        &self,
        session_id: &str,
        title: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .patch_json(
                &format!("/api/sessions/{}", url_encode(session_id)),
                serde_json::json!({ "title": title }),
            )
            .await?;
        validate_session_json_identity(session_id, &value, "rename session receipt")?;
        Ok(value)
    }

    pub async fn update_session_model(
        &self,
        session_id: &str,
        model: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .patch_json(
                &format!("/api/sessions/{}", url_encode(session_id)),
                serde_json::json!({ "model": model }),
            )
            .await?;
        validate_session_json_identity(session_id, &value, "update session receipt")?;
        Ok(value)
    }

    pub async fn delete_session(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.delete_json(&format!("/api/sessions/{}", url_encode(session_id)))
            .await
    }

    pub async fn branch_session(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/sessions/{}/branch", url_encode(session_id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn session_projection(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .get_json(&format!(
                "/api/sessions/{}/projection",
                url_encode(session_id)
            ))
            .await?;
        validate_session_json_identity(session_id, &value, "session projection")?;
        Ok(value)
    }

    pub async fn session_input_projection(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .get_json(&format!(
                "/api/sessions/{}/input-projection",
                url_encode(session_id)
            ))
            .await?;
        validate_session_json_identity(session_id, &value, "session input projection")?;
        Ok(value)
    }

    pub async fn cancel_session_input(
        &self,
        session_id: &str,
        input_id: &str,
        reason: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .post_json(
                &format!(
                    "/api/sessions/{}/inputs/{}/cancel",
                    url_encode(session_id),
                    url_encode(input_id)
                ),
                serde_json::json!({ "reason": reason }),
            )
            .await?;
        validate_session_json_identity(session_id, &value, "cancel session input receipt")?;
        Ok(value)
    }

    pub async fn turn_inbox(
        &self,
        session_id: &str,
        turn_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let suffix = turn_id
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!("?turn_id={}", url_encode(value)))
            .unwrap_or_default();
        let value = self
            .get_json(&format!(
                "/api/sessions/{}/turn-inbox{}",
                url_encode(session_id),
                suffix
            ))
            .await?;
        validate_session_json_identity(session_id, &value, "turn inbox")?;
        Ok(value)
    }

    pub async fn ensure_session(
        &self,
        session_id: &str,
        model: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        require_gateway_session_operation_ok(
            self.post_json(
                &format!("/api/sessions/{}/ensure", url_encode(session_id)),
                serde_json::json!({ "model": model }),
            )
            .await?,
            "ensure session",
            session_id,
        )
    }

    pub async fn session_messages(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<SessionMessagesPage, GatewayApiError> {
        let value = self
            .get_json(&format!(
                "/api/sessions/{}/messages?from_seq={from_sequence}&limit={}",
                url_encode(session_id),
                limit.min(500)
            ))
            .await?;
        let page: SessionMessagesPage = serde_json::from_value(value).map_err(|error| {
            GatewayApiError::Contract(format!("invalid session message page: {error}"))
        })?;
        validate_session_messages_identity(session_id, &page)?;
        Ok(page)
    }

    pub async fn session_messages_offset(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<SessionMessagesPage, GatewayApiError> {
        let value = self
            .get_json(&format!(
                "/api/sessions/{}/messages?offset={offset}&limit={}",
                url_encode(session_id),
                limit.min(500)
            ))
            .await?;
        let page: SessionMessagesPage = serde_json::from_value(value).map_err(|error| {
            GatewayApiError::Contract(format!("invalid session message page: {error}"))
        })?;
        validate_session_messages_identity(session_id, &page)?;
        Ok(page)
    }

    pub async fn hydrate_session_history(
        &self,
        session_id: &str,
        tx: CowdEventSender,
        next_sequence: Arc<AtomicUsize>,
        authority_generation: u64,
    ) {
        hydrate_session_history_with_retry(
            self,
            session_id,
            tx,
            next_sequence,
            authority_generation,
        )
        .await;
    }

    pub async fn session_history_index(
        &self,
        session_id: &str,
    ) -> Result<crate::protocol::SessionHistoryIndexProjection, GatewayApiError> {
        let value = self
            .get_json(&format!(
                "/api/sessions/{}/history-index?metadata_limit=128&card_limit=64",
                url_encode(session_id)
            ))
            .await?;
        let projection =
            serde_json::from_value::<crate::protocol::SessionHistoryIndexProjection>(value)
                .map_err(|error| {
                    GatewayApiError::Contract(format!("invalid session history index: {error}"))
                })?;
        if projection.session_id != session_id {
            return Err(GatewayApiError::Contract(format!(
                "requested session `{session_id}` but Gateway returned history index for `{}`",
                projection.session_id
            )));
        }
        Ok(projection)
    }

    pub async fn session_execution_index(
        &self,
        session_id: &str,
    ) -> Result<crate::protocol::SessionExecutionIndexProjection, GatewayApiError> {
        let value = self
            .get_json(&format!(
                "/api/sessions/{}/execution",
                url_encode(session_id)
            ))
            .await?;
        let index: crate::protocol::SessionExecutionIndexProjection = serde_json::from_value(value)
            .map_err(|error| {
                GatewayApiError::Contract(format!("invalid session execution index: {error}"))
            })?;
        if index.session_id != session_id {
            return Err(GatewayApiError::Contract(format!(
                "requested session `{session_id}` but Gateway returned execution index for `{}`",
                index.session_id
            )));
        }
        Ok(index)
    }

    pub async fn send_message(
        &self,
        session_id: &str,
        content: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.send_message_with_resources(session_id, content, &[], None)
            .await
    }

    pub async fn send_message_with_resources(
        &self,
        session_id: &str,
        content: &str,
        resource_ids: &[String],
        client_message_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .post_json(
                &format!("/api/sessions/{}/messages", url_encode(session_id)),
                serde_json::json!({
                    "content": content,
                    "resource_ids": resource_ids,
                    "client_message_id": client_message_id,
                    // The caller already supplies the surface-qualified stable
                    // identity (`tui:<uuid>`). Reuse it as the admission key so
                    // optimistic UI, durable message and idempotent retry all
                    // address one identity instead of `tui:tui:<uuid>`.
                    "idempotency_key": client_message_id,
                }),
            )
            .await?;
        validate_session_json_identity(session_id, &value, "message admission receipt")?;
        Ok(value)
    }

    pub async fn upload_resource_path(
        &self,
        path: &Path,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let bytes = tokio::fs::metadata(path)
            .await
            .map_err(|error| GatewayApiError::Url(error.to_string()))?
            .len();
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|error| GatewayApiError::Url(error.to_string()))?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("resource.bin")
            .to_string();
        let body =
            reqwest::Body::wrap_stream(futures::stream::try_unfold(file, |mut file| async move {
                let mut chunk = vec![0_u8; 64 * 1024];
                let read = file.read(&mut chunk).await?;
                if read == 0 {
                    Ok::<_, std::io::Error>(None)
                } else {
                    chunk.truncate(read);
                    Ok(Some((chunk, file)))
                }
            }));
        let part = reqwest::multipart::Part::stream_with_length(body, bytes).file_name(file_name);
        let form = reqwest::multipart::Form::new()
            .text("source", "tui")
            .text("session_id", session_id.to_string())
            .part("file", part);
        let url = format!("{}/api/resources", self.base_url);
        let request = self.authorize(self.client.post(url).multipart(form));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        let text = response.text().await.map_err(GatewayApiError::Http)?;
        if !status.is_success() {
            return Err(gateway_status_error(status, text));
        }
        let value: serde_json::Value = serde_json::from_str(&text).map_err(|error| {
            GatewayApiError::Contract(format!("invalid resource receipt: {error}"))
        })?;
        validate_session_json_identity_at(
            session_id,
            &value,
            "resource upload receipt",
            &["/resource/session_id"],
        )?;
        Ok(value)
    }

    pub async fn workspace_overview(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/workspace").await
    }

    pub async fn workspace_files(
        &self,
        dir: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let path = match dir.map(str::trim).filter(|dir| !dir.is_empty()) {
            Some(dir) => format!("/api/workspace/files?dir={}", url_encode(dir)),
            None => "/api/workspace/files".to_string(),
        };
        self.get_json(&path).await
    }

    pub async fn workspace_files_recursive(
        &self,
        dir: Option<&str>,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let mut path = match dir.map(str::trim).filter(|dir| !dir.is_empty()) {
            Some(dir) => format!("/api/workspace/files?dir={}", url_encode(dir)),
            None => "/api/workspace/files?".to_string(),
        };
        if !path.ends_with('?') && !path.ends_with('&') {
            path.push('&');
        }
        path.push_str("recursive=true&limit=");
        path.push_str(&limit.to_string());
        self.get_json(&path).await
    }

    pub async fn create_workspace_file(
        &self,
        path: &str,
        content: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/workspace/files",
            serde_json::json!({ "path": path, "content": content }),
        )
        .await
    }

    pub async fn create_workspace_dir(
        &self,
        path: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/workspace/dirs", serde_json::json!({ "path": path }))
            .await
    }

    pub async fn delete_workspace_path(
        &self,
        path: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.delete_json(&format!("/api/workspace/files?path={}", url_encode(path)))
            .await
    }

    pub async fn rename_workspace_path(
        &self,
        path: &str,
        to: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/workspace/rename",
            serde_json::json!({ "path": path, "to": to }),
        )
        .await
    }

    pub async fn workspace_meta(&self, path: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/workspace/meta?path={}", url_encode(path)))
            .await
    }

    pub async fn download_workspace_path(&self, path: &str) -> Result<Vec<u8>, GatewayApiError> {
        self.get_bytes(&format!(
            "/api/workspace/download?path={}",
            url_encode(path)
        ))
        .await
    }

    pub async fn workspace_file_preview(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let bytes = self
            .get_bytes(&format!("/api/file/raw?path={}", url_encode(path)))
            .await?;
        let truncated = bytes.len() > max_bytes;
        let slice = if truncated {
            &bytes[..max_bytes]
        } else {
            bytes.as_slice()
        };
        let content = String::from_utf8_lossy(slice).to_string();
        Ok(serde_json::json!({
            "path": path,
            "content": content,
            "bytes": bytes.len(),
            "truncated": truncated,
        }))
    }

    pub async fn upload_workspace_file_path(
        &self,
        path: &Path,
        dir: Option<&str>,
        overwrite: bool,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let bytes = std::fs::read(path).map_err(|error| GatewayApiError::Url(error.to_string()))?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("upload.bin")
            .to_string();
        let part = reqwest::multipart::Part::bytes(bytes).file_name(file_name);
        let form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("dir", dir.unwrap_or_default().to_string())
            .text("overwrite", overwrite.to_string());
        let url = format!("{}/api/upload", self.base_url);
        let request = self.authorize(self.client.post(url).multipart(form));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        let text = response.text().await.map_err(GatewayApiError::Http)?;
        if !status.is_success() {
            return Err(gateway_status_error(status, text));
        }
        serde_json::from_str(&text).map_err(|error| GatewayApiError::Url(error.to_string()))
    }

    pub async fn list_session_attachments(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .get_json(&format!(
                "/api/sessions/{}/attachments",
                url_encode(session_id)
            ))
            .await?;
        validate_session_json_identity(session_id, &value, "session attachment list")?;
        Ok(value)
    }

    pub async fn add_session_attachment(
        &self,
        session_id: &str,
        path: &str,
        kind: &str,
        label: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .post_json(
                &format!("/api/sessions/{}/attachments", url_encode(session_id)),
                serde_json::json!({
                    "path": path,
                    "kind": kind,
                    "label": label,
                }),
            )
            .await?;
        validate_session_json_identity(session_id, &value, "add session attachment receipt")?;
        Ok(value)
    }

    pub async fn delete_session_attachment(
        &self,
        session_id: &str,
        ref_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .delete_json(&format!(
                "/api/sessions/{}/attachments/{}",
                url_encode(session_id),
                url_encode(ref_id)
            ))
            .await?;
        validate_session_json_identity(session_id, &value, "delete session attachment receipt")?;
        Ok(value)
    }

    pub async fn cancel_session_turn(
        &self,
        session_id: &str,
        reason: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .post_json(
                &format!("/api/sessions/{}/cancel", url_encode(session_id)),
                serde_json::json!({ "reason": reason }),
            )
            .await?;
        validate_session_json_identity(session_id, &value, "cancel session turn receipt")?;
        Ok(value)
    }

    pub async fn consume_session_live_source(
        &self,
        session_id: &str,
        tx: CowdEventSender,
        after_commit_cursor: Option<u64>,
        next_message_sequence: Arc<AtomicUsize>,
        authority_generation: u64,
    ) -> Result<SessionStreamProgress, GatewayApiError> {
        let mut source = self
            .live
            .subscribe(harness_contract::live::LiveSourceSelector {
                kind: harness_contract::live::LiveSourceKind::Session,
                id: session_id.to_string(),
                cursor: after_commit_cursor.unwrap_or_default(),
                revision: 0,
                detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
            })
            .await?;
        tx.send_wait(session_scoped_event(
            session_id,
            authority_generation,
            CowdEvent::SessionStreamConnection {
                session_id: session_id.to_string(),
                state: SessionStreamConnectionState::Connected,
            },
        ))
        .await
        .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))?;
        let mut latest_cursor = after_commit_cursor;
        while let Some(envelope) = source.recv().await {
            if envelope.source_kind != "session" || envelope.source_id != session_id {
                return Err(GatewayApiError::Contract(
                    "TUI live demultiplexer delivered a mismatched Session source".to_string(),
                ));
            }
            let candidate_cursor = envelope.source_cursor;
            if envelope.event == "source.authorization_revoked" {
                let reason = envelope
                    .payload
                    .get("reason")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("Gateway revoked this Session source")
                    .to_string();
                tx.send_wait(session_scoped_event(
                    session_id,
                    authority_generation,
                    CowdEvent::SessionAuthorizationRevoked {
                        session_id: session_id.to_string(),
                        reason: reason.clone(),
                    },
                ))
                .await
                .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))?;
                return Err(GatewayApiError::SessionAuthorizationRevoked(reason));
            }
            if envelope.source_health == harness_contract::live::SourceHealth::ResyncRequired {
                latest_cursor = Some(
                    latest_cursor
                        .unwrap_or_default()
                        .max(candidate_cursor.unwrap_or_default()),
                );
                let _ = tx.send(session_scoped_event(
                    session_id,
                    authority_generation,
                    CowdEvent::Warning {
                        message: format!(
                            "Gateway Session source requested recovery: {}; refreshing durable history and execution projection",
                            envelope
                                .payload
                                .get("reason")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("canonical resync required")
                        ),
                    },
                ));
                return Ok(SessionStreamProgress {
                    commit_cursor: latest_cursor,
                    next_message_sequence: next_message_sequence.load(Ordering::Acquire),
                });
            }
            let frame = match candidate_cursor {
                Some(cursor) => format!(
                    "id: {cursor}\nevent: {}\ndata: {}",
                    envelope.event, envelope.payload
                ),
                None => format!("event: {}\ndata: {}", envelope.event, envelope.payload),
            };
            match strict_gateway_sse_frame_to_cowd_event_for_session(&frame, session_id) {
                Ok(Some(event)) => {
                    deliver_session_stream_event_with_catchup(
                        self,
                        &tx,
                        session_id,
                        event,
                        &next_message_sequence,
                        authority_generation,
                    )
                    .await?;
                    latest_cursor = Some(
                        latest_cursor
                            .unwrap_or_default()
                            .max(candidate_cursor.unwrap_or_default()),
                    );
                }
                Ok(None) => {}
                Err(error) => return Err(GatewayApiError::Contract(error)),
            }
        }
        Ok(SessionStreamProgress {
            commit_cursor: latest_cursor,
            next_message_sequence: next_message_sequence.load(Ordering::Acquire),
        })
    }

    pub async fn attach_session(
        &self,
        session_id: &str,
        surface: &str,
        role: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        require_gateway_session_operation_ok(
            self.post_json(
                &format!("/api/sessions/{}/attach", url_encode(session_id)),
                serde_json::json!({ "surface": surface, "role": role }),
            )
            .await?,
            "attach session",
            session_id,
        )
    }

    pub async fn detach_session(
        &self,
        session_id: &str,
        surface: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        require_gateway_session_operation_ok(
            self.post_json(
                &format!("/api/sessions/{}/detach", url_encode(session_id)),
                serde_json::json!({ "surface": surface }),
            )
            .await?,
            "detach session",
            session_id,
        )
    }

    pub async fn lifecycle_snapshot(
        &self,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        match session_id {
            Some(session_id) => {
                let value = self
                    .get_json(&format!(
                        "/api/sessions/{}/lifecycle",
                        url_encode(session_id)
                    ))
                    .await?;
                validate_session_json_identity(session_id, &value, "session lifecycle snapshot")?;
                Ok(value)
            }
            None => self.get_json("/api/runtime/snapshot").await,
        }
    }

    pub async fn replay_session(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .get_json(&format!(
                "/api/sessions/{}/replay?from_sequence={from_sequence}&limit={limit}",
                url_encode(session_id)
            ))
            .await?;
        validate_session_json_identity(session_id, &value, "session replay")?;
        Ok(value)
    }

    pub async fn cowd_capabilities(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/capabilities").await
    }

    pub async fn cowd_projection(
        &self,
        surface: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/cowd/projection?surface={}",
            url_encode(surface)
        ))
        .await
    }

    pub async fn cowd_surfaces(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/surfaces").await
    }

    pub async fn cowd_release_gate(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/release-gate").await
    }

    pub async fn gateway_capability_contract(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/gateway/capability-contract").await
    }

    pub async fn gateway_openai_tools(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/gateway/openai-tools").await
    }

    pub async fn structured_sources(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/structured/sources").await
    }

    pub async fn structured_facts(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/structured/facts").await
    }

    pub async fn structured_evidence(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/structured/evidence").await
    }

    pub async fn structured_watermarks(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/structured/watermarks").await
    }

    pub async fn structured_ingest_plan(
        &self,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/cowd/structured/ingest-plan", input)
            .await
    }

    pub async fn runtime_session_leases(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/session-leases").await
    }

    pub async fn acquire_runtime_session_lease(
        &self,
        session_id: &str,
        mode: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        require_gateway_session_operation_ok(
            self.post_json(
                "/api/runtime/session-leases/acquire",
                serde_json::json!({
                    "session_id": session_id,
                    "mode": mode,
                }),
            )
            .await?,
            "acquire runtime session lease",
            session_id,
        )
    }

    pub async fn release_runtime_session_lease(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        require_gateway_session_operation_ok(
            self.post_json(
                "/api/runtime/session-leases/release",
                serde_json::json!({
                    "session_id": session_id,
                }),
            )
            .await?,
            "release runtime session lease",
            session_id,
        )
    }

    pub async fn runtime_effective_config(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/config/effective").await
    }

    pub async fn config(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/config").await
    }

    pub async fn config_providers(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/config/providers").await
    }

    pub async fn update_config_model(
        &self,
        model: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.put_json("/api/config", serde_json::json!({ "model": model }))
            .await
    }

    pub async fn config_reload_status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/config/reload/status").await
    }

    pub async fn runtime_timeline(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/runtime/timeline?session_id={}&limit={}",
            url_encode(session_id),
            limit
        ))
        .await
    }

    pub async fn execution_projection(
        &self,
        execution_id: &str,
        full: bool,
    ) -> Result<harness_contract::projection::ExecutionProjection, GatewayApiError> {
        let scope = if full { "full" } else { "summary" };
        let value = self
            .get_json(&format!(
                "/api/runtime/executions/{}?detail_scope={scope}",
                url_encode(execution_id)
            ))
            .await?;
        let projection: harness_contract::projection::ExecutionProjection =
            serde_json::from_value(value)
                .map_err(|error| GatewayApiError::Contract(error.to_string()))?;
        crate::protocol::validate_execution_projection_schema(&projection)
            .map_err(GatewayApiError::Contract)?;
        validate_execution_projection_identity(execution_id, &projection.execution_id)?;
        Ok(projection)
    }

    pub async fn consume_execution_live_source(
        &self,
        execution_id: &str,
        after_cursor: u64,
        after_revision: u64,
        full: bool,
        generation: u64,
        tx: CowdEventSender,
    ) -> Result<(u64, u64), GatewayApiError> {
        let mut source = self
            .live
            .subscribe(harness_contract::live::LiveSourceSelector {
                kind: harness_contract::live::LiveSourceKind::Execution,
                id: execution_id.to_string(),
                cursor: after_cursor,
                revision: after_revision,
                detail_scope: if full {
                    harness_contract::projection::ProjectionDetailScope::Full
                } else {
                    harness_contract::projection::ProjectionDetailScope::Summary
                },
            })
            .await?;
        tx.send(CowdEvent::ExecutionProjectionConnection {
            generation,
            execution_id: execution_id.to_string(),
            state: crate::protocol::SessionStreamConnectionState::Connected,
        })
        .map_err(|_| {
            GatewayApiError::Url(
                "TUI execution projection consumer closed during connect".to_string(),
            )
        })?;

        let mut latest_cursor = after_cursor;
        let mut latest_revision = after_revision;
        while let Some(envelope) = source.recv().await {
            if envelope.source_kind != "execution" || envelope.source_id != execution_id {
                return Err(GatewayApiError::Contract(
                    "TUI live demultiplexer delivered a mismatched Execution source".to_string(),
                ));
            }
            if envelope.event == "source.authorization_revoked" {
                return Err(GatewayApiError::Status(
                    reqwest::StatusCode::FORBIDDEN,
                    envelope
                        .payload
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("execution source authorization revoked")
                        .to_string(),
                ));
            }
            if envelope.source_health == harness_contract::live::SourceHealth::ResyncRequired {
                let snapshot = self.execution_projection(execution_id, full).await?;
                latest_cursor = snapshot.cursor;
                latest_revision = snapshot.revision;
                tx.send_wait(CowdEvent::ExecutionProjectionLoaded {
                    generation,
                    projection: snapshot,
                })
                .await
                .map_err(|_| {
                    GatewayApiError::Url(
                        "TUI execution projection consumer closed during resync".to_string(),
                    )
                })?;
                return Ok((latest_cursor, latest_revision));
            }
            latest_revision = match envelope.event.as_str() {
                "projection_snapshot" => envelope
                    .payload
                    .get("revision")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(latest_revision),
                "projection_delta" => envelope
                    .payload
                    .get("target_revision")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(latest_revision),
                _ => latest_revision,
            };
            let frame = format!("event: {}\ndata: {}", envelope.event, envelope.payload);
            latest_cursor = self
                .apply_execution_projection_sse_frame(
                    &frame,
                    execution_id,
                    full,
                    generation,
                    latest_cursor,
                    &tx,
                )
                .await?;
        }
        Ok((latest_cursor, latest_revision))
    }

    pub async fn consume_mission_live_source(
        &self,
        mission_id: &str,
        tx: CowdEventSender,
    ) -> Result<(), GatewayApiError> {
        let mut source = self
            .live
            .subscribe(harness_contract::live::LiveSourceSelector {
                kind: harness_contract::live::LiveSourceKind::Mission,
                id: mission_id.to_string(),
                cursor: 0,
                revision: 0,
                detail_scope: harness_contract::projection::ProjectionDetailScope::Full,
            })
            .await?;
        let mut applied_cursor = None;
        let mut applied_revision = None;
        while let Some(envelope) = source.recv().await {
            if envelope.source_kind != "mission" || envelope.source_id != mission_id {
                return Err(GatewayApiError::Contract(
                    "TUI live demultiplexer delivered a mismatched Mission source".to_string(),
                ));
            }
            if envelope.event == "source.authorization_revoked" {
                return Err(GatewayApiError::Status(
                    reqwest::StatusCode::FORBIDDEN,
                    envelope
                        .payload
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("mission source authorization revoked")
                        .to_string(),
                ));
            }
            if envelope.source_health == harness_contract::live::SourceHealth::ResyncRequired {
                let snapshot = self.mission_control_snapshot().await?;
                applied_cursor = Some(snapshot.cursor);
                applied_revision = Some(snapshot.revision);
                tx.send_wait(CowdEvent::MissionProjectionSnapshot {
                    mission_id: mission_id.to_string(),
                    snapshot,
                })
                .await
                .map_err(|_| {
                    GatewayApiError::Url("TUI Mission projection consumer closed".to_string())
                })?;
                continue;
            }
            if envelope.event == "mission_snapshot" {
                let snapshot = serde_json::from_value::<
                    harness_contract::mission::MissionMaterializedSnapshot,
                >(envelope.payload)
                .map_err(|error| {
                    GatewayApiError::Contract(format!(
                        "Gateway Mission snapshot contract is invalid: {error}"
                    ))
                })?;
                applied_cursor = Some(snapshot.cursor);
                applied_revision = Some(snapshot.revision);
                tx.send_wait(CowdEvent::MissionProjectionSnapshot {
                    mission_id: mission_id.to_string(),
                    snapshot,
                })
                .await
                .map_err(|_| {
                    GatewayApiError::Url("TUI Mission projection consumer closed".to_string())
                })?;
            } else if envelope.event == "mission_delta" {
                let delta = serde_json::from_value::<
                    harness_contract::mission::MissionProjectionDelta,
                >(envelope.payload)
                .map_err(|error| {
                    GatewayApiError::Contract(format!(
                        "Gateway Mission delta contract is invalid: {error}"
                    ))
                })?;
                if applied_cursor != Some(delta.from_cursor)
                    || applied_revision != delta.from_revision
                {
                    let snapshot = self.mission_control_snapshot().await?;
                    applied_cursor = Some(snapshot.cursor);
                    applied_revision = Some(snapshot.revision);
                    tx.send_wait(CowdEvent::MissionProjectionSnapshot {
                        mission_id: mission_id.to_string(),
                        snapshot,
                    })
                    .await
                    .map_err(|_| {
                        GatewayApiError::Url("TUI Mission projection consumer closed".to_string())
                    })?;
                    continue;
                }
                applied_cursor = Some(delta.to_cursor);
                applied_revision = Some(delta.revision);
                tx.send_wait(CowdEvent::MissionProjectionDelta {
                    mission_id: mission_id.to_string(),
                    delta,
                })
                .await
                .map_err(|_| {
                    GatewayApiError::Url("TUI Mission projection consumer closed".to_string())
                })?;
            }
        }
        Ok(())
    }

    async fn apply_execution_projection_sse_frame(
        &self,
        frame: &str,
        execution_id: &str,
        full: bool,
        generation: u64,
        latest_cursor: u64,
        tx: &CowdEventSender,
    ) -> Result<u64, GatewayApiError> {
        let event_name = gateway_sse_frame_event_name(frame);
        if event_name.is_none()
            && frame
                .lines()
                .all(|line| line.trim().is_empty() || line.trim_start().starts_with(':'))
        {
            return Ok(latest_cursor);
        }
        if gateway_sse_frame_projection_authorization_revoked(frame) {
            return Err(GatewayApiError::Status(
                reqwest::StatusCode::FORBIDDEN,
                "Gateway revoked the execution projection stream".to_string(),
            ));
        }
        let frame_cursor = gateway_sse_frame_commit_cursor(frame);
        match event_name {
            Some("projection_snapshot") => {
                let data = gateway_sse_frame_data(frame).ok_or_else(|| {
                    GatewayApiError::Contract(
                        "projection_snapshot frame has no typed payload".to_string(),
                    )
                })?;
                let snapshot: harness_contract::projection::ExecutionProjection =
                    serde_json::from_str(&data).map_err(|error| {
                        GatewayApiError::Contract(format!(
                            "projection_snapshot payload is invalid: {error}"
                        ))
                    })?;
                crate::protocol::validate_execution_projection_schema(&snapshot)
                    .map_err(GatewayApiError::Contract)?;
                if snapshot.execution_id != execution_id {
                    return Err(GatewayApiError::Contract(format!(
                        "projection_snapshot execution mismatch: expected {execution_id}, got {}",
                        snapshot.execution_id
                    )));
                }
                if frame_cursor.is_some_and(|cursor| cursor < snapshot.cursor) {
                    return Err(GatewayApiError::Contract(
                        "projection_snapshot frame id regressed behind its typed cursor"
                            .to_string(),
                    ));
                }
                let accepted_cursor = latest_cursor
                    .max(snapshot.cursor)
                    .max(frame_cursor.unwrap_or_default());
                tx.send_wait(CowdEvent::ExecutionProjectionLoaded {
                    generation,
                    projection: snapshot,
                })
                .await
                .map_err(|_| {
                    GatewayApiError::Url(
                        "TUI execution projection consumer closed during snapshot".to_string(),
                    )
                })?;
                Ok(accepted_cursor)
            }
            Some("projection_delta") => {
                let data = gateway_sse_frame_data(frame).ok_or_else(|| {
                    GatewayApiError::Contract(
                        "projection_delta frame has no typed payload".to_string(),
                    )
                })?;
                let delta: harness_contract::projection::ProjectionDelta =
                    serde_json::from_str(&data).map_err(|error| {
                        GatewayApiError::Contract(format!(
                            "projection_delta payload is invalid: {error}"
                        ))
                    })?;
                crate::protocol::validate_projection_delta_schema(&delta)
                    .map_err(GatewayApiError::Contract)?;
                if delta.execution_id != execution_id {
                    return Err(GatewayApiError::Contract(format!(
                        "projection_delta execution mismatch: expected {execution_id}, got {}",
                        delta.execution_id
                    )));
                }
                if frame_cursor.is_some_and(|cursor| cursor < delta.target_cursor) {
                    return Err(GatewayApiError::Contract(
                        "projection_delta frame id regressed behind its target cursor".to_string(),
                    ));
                }
                let accepted_cursor = latest_cursor
                    .max(delta.target_cursor)
                    .max(frame_cursor.unwrap_or_default());
                tx.send_wait(CowdEvent::ExecutionProjectionDelta { generation, delta })
                    .await
                    .map_err(|_| {
                        GatewayApiError::Url(
                            "TUI execution projection consumer closed during delta".to_string(),
                        )
                    })?;
                Ok(accepted_cursor)
            }
            Some("projection_live") => {
                let data = gateway_sse_frame_data(frame).ok_or_else(|| {
                    GatewayApiError::Contract(
                        "projection_live frame has no typed payload".to_string(),
                    )
                })?;
                let update: harness_contract::projection::ExecutionLiveUpdate =
                    serde_json::from_str(&data).map_err(|error| {
                        GatewayApiError::Contract(format!(
                            "projection_live payload is invalid: {error}"
                        ))
                    })?;
                if update.schema_version
                    != harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION
                    || update.execution_id != execution_id
                {
                    return Err(GatewayApiError::Contract(
                        "projection_live schema or execution identity mismatch".to_string(),
                    ));
                }
                tx.send_wait(CowdEvent::ExecutionProjectionLive { generation, update })
                    .await
                    .map_err(|_| {
                        GatewayApiError::Url(
                            "TUI execution projection consumer closed during live update"
                                .to_string(),
                        )
                    })?;
                Ok(latest_cursor)
            }
            Some("projection_resync") => {
                // Fetch canonical state without terminating/restarting this
                // healthy SSE task. Generation gating drops a late snapshot
                // if the user selected a different execution meanwhile.
                let snapshot = self.execution_projection(execution_id, full).await?;
                let accepted_cursor = latest_cursor.max(snapshot.cursor);
                tx.send_wait(CowdEvent::ExecutionProjectionLoaded {
                    generation,
                    projection: snapshot,
                })
                .await
                .map_err(|_| {
                    GatewayApiError::Url(
                        "TUI execution projection consumer closed during resync".to_string(),
                    )
                })?;
                Ok(accepted_cursor)
            }
            Some("projection_error") => Err(GatewayApiError::Contract(
                "Gateway execution projection stream reported projection_error".to_string(),
            )),
            Some(other) => Err(GatewayApiError::Contract(format!(
                "Gateway execution projection stream emitted unknown event `{other}`"
            ))),
            None => Err(GatewayApiError::Contract(
                "Gateway execution projection frame has no event type".to_string(),
            )),
        }
    }

    pub async fn execute_projection_command(
        &self,
        execution_id: &str,
        request: &harness_contract::projection::ExecutionCommandRequest,
    ) -> Result<harness_contract::projection::ExecutionCommandReceipt, GatewayApiError> {
        let body = serde_json::to_value(request)
            .map_err(|error| GatewayApiError::Url(error.to_string()))?;
        let value = self
            .post_json(
                &format!(
                    "/api/runtime/executions/{}/commands",
                    url_encode(execution_id)
                ),
                body,
            )
            .await?;
        serde_json::from_value(value).map_err(|error| GatewayApiError::Url(error.to_string()))
    }

    pub async fn current_context(
        &self,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let path = match session_id {
            Some(id) if !id.trim().is_empty() => {
                format!("/api/context/current?session_id={}", url_encode(id))
            }
            _ => "/api/context/current".to_string(),
        };
        self.get_json(&path).await
    }

    pub async fn memory_status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/memory/status").await
    }

    pub async fn memory_maintenance(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/memory/maintenance").await
    }

    pub async fn run_memory_maintenance(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/memory/maintenance", serde_json::json!({}))
            .await
    }

    pub async fn memory_knowledge_candidates(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/memory/knowledge/candidates").await
    }

    pub async fn reality_status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/reality/status").await
    }

    pub async fn reality_capabilities(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/reality/capabilities").await
    }

    pub async fn reality_flow(
        &self,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let path = match session_id.map(str::trim).filter(|id| !id.is_empty()) {
            Some(id) => format!("/api/reality/flow?session_id={}", url_encode(id)),
            None => "/api/reality/flow".to_string(),
        };
        self.get_json(&path).await
    }

    pub async fn reality_boundaries(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/reality/boundaries").await
    }

    pub async fn reality_governance(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/reality/governance").await
    }

    pub async fn task_status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/tasks").await
    }

    pub async fn pending_approvals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/approval/pending").await
    }

    pub async fn approval_history(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/approval/history?limit=200&offset=0")
            .await
    }

    pub async fn approval_exact(
        &self,
        approval_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/approval/{}", url_encode(approval_id)))
            .await
    }

    pub async fn mission_projection(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/mission/projection").await
    }

    pub async fn mission_control(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/mission/control").await
    }

    async fn mission_control_snapshot(
        &self,
    ) -> Result<harness_contract::mission::MissionMaterializedSnapshot, GatewayApiError> {
        let value = self.mission_control().await?;
        let snapshot = value.get("snapshot").cloned().unwrap_or(value);
        serde_json::from_value(snapshot).map_err(|error| {
            GatewayApiError::Contract(format!(
                "Gateway Mission materialized snapshot contract is invalid: {error}"
            ))
        })
    }

    pub async fn tick_mission_schedules(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/mission/schedules/tick", body).await
    }

    pub async fn apply_runtime_recovery(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/runtime/events/recover", serde_json::json!({}))
            .await
    }

    pub async fn mission_session_detail(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/mission/sessions/{}", url_encode(session_id)))
            .await
    }

    pub async fn mission_approvals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/mission/approvals").await
    }

    pub async fn mission_relations(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/mission/relations").await
    }

    pub async fn submit_mission_approval(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/mission/approvals", body).await
    }

    pub async fn execute_mission_command(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/mission/control", body).await
    }

    /// Read the Runtime-owned catalog of runnable Team template revisions.
    pub async fn team_templates(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/team-templates").await
    }

    /// Submit declarative Team intent. Gateway forwards it to Runtime, which
    /// resolves template/Agent revisions and constructs the graph.
    pub async fn instantiate_team_template(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/team-templates/instantiate", body)
            .await
    }

    pub async fn team_working_state(
        &self,
        team_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/runtime/teams/{}/working-state",
            url_encode(team_id)
        ))
        .await
    }

    pub async fn decide_mission_approval(
        &self,
        approval_id: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/mission/approvals/{}/decision",
                url_encode(approval_id)
            ),
            body,
        )
        .await
    }

    pub async fn upsert_mission_proxy(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/mission/proxies", body).await
    }

    pub async fn runtime_agent_input(
        &self,
        agent_id: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/runtime/agents/{}/input", url_encode(agent_id)),
            serde_json::json!({ "payload": payload }),
        )
        .await
    }

    pub async fn runtime_agent_interrupt(
        &self,
        agent_id: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/runtime/agents/{}/interrupt", url_encode(agent_id)),
            serde_json::json!({ "payload": payload }),
        )
        .await
    }

    pub async fn runtime_agent_shutdown(
        &self,
        agent_id: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/runtime/agents/{}/shutdown", url_encode(agent_id)),
            serde_json::json!({ "payload": payload }),
        )
        .await
    }

    pub async fn respond_approval(
        &self,
        id: &str,
        approved: bool,
        persistence: Option<&str>,
        reason: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/approval/respond",
            serde_json::json!({
                "id": id,
                "approved": approved,
                "persistence": persistence.unwrap_or("once"),
                "reason": reason,
            }),
        )
        .await
    }

    pub async fn cancel_task(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/tasks/{}/cancel", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn complete_task(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/tasks/{}/complete", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn cross_plane_summary(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cross-plane/summary").await
    }

    pub async fn cross_plane_execution_receipt(
        &self,
        receipt_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let path = format!(
            "/api/cross-plane/action/executions/{}",
            url_encode(receipt_id)
        );
        let response: serde_json::Value = self.get_json(&path).await?;
        Ok(response
            .get("execution_receipt")
            .cloned()
            .unwrap_or(response))
    }

    pub async fn connector_accounts(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/connectors/accounts").await
    }

    pub async fn connector_capabilities(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/connectors/capabilities").await
    }

    pub async fn connector_resources(
        &self,
        query: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let mut path = format!("/api/connectors/resources?limit={limit}&offset={offset}");
        if let Some(query) = query.map(str::trim).filter(|query| !query.is_empty()) {
            path.push_str("&q=");
            path.push_str(&url_encode(query));
        }
        self.get_json(&path).await
    }

    pub async fn message_connectors(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/message-connectors").await
    }

    pub async fn message_connector_status(
        &self,
        name: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/message-connectors/{}/status",
            url_encode(name)
        ))
        .await
    }

    pub async fn message_connector_repair(
        &self,
        name: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/message-connectors/{}/repair", url_encode(name)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn message_endpoints(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/message-endpoints").await
    }

    pub async fn message_routes(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/message-routes").await
    }

    pub async fn message_bindings(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/message-bindings").await
    }

    pub async fn surface_registry(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/surfaces").await
    }

    pub async fn surface_health_summary(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/surfaces/health").await
    }

    pub async fn surface_detail(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}", url_encode(id)))
            .await
    }

    pub async fn surface_routes(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/routes", url_encode(id)))
            .await
    }

    pub async fn surface_resources(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/resources", url_encode(id)))
            .await
    }

    pub async fn surface_status(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/status", url_encode(id)))
            .await
    }

    pub async fn surface_health(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/health", url_encode(id)))
            .await
    }

    pub async fn surface_health_check(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/health-check", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_start(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/start", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_stop(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/stop", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_restart(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/restart", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_repair(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/repair", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_events(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/events", url_encode(id)))
            .await
    }

    pub async fn surface_inbox(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/inbox", url_encode(id)))
            .await
    }

    pub async fn surface_outbox(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/outbox", url_encode(id)))
            .await
    }

    pub async fn surface_messages(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/messages", url_encode(id)))
            .await
    }

    pub async fn surface_archive_messages(
        &self,
        id: &str,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/messages/archive", url_encode(id)),
            serde_json::json!({ "limit": limit }),
        )
        .await
    }

    pub async fn surface_purge_archived_events(
        &self,
        id: &str,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/surfaces/{}/messages/purge-archived-events",
                url_encode(id)
            ),
            serde_json::json!({ "limit": limit }),
        )
        .await
    }

    pub async fn surface_deliveries(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/deliveries", url_encode(id)))
            .await
    }

    pub async fn surface_outbox_delivery(
        &self,
        id: &str,
        delivery_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/surfaces/{}/outbox/{}",
            url_encode(id),
            url_encode(delivery_id)
        ))
        .await
    }

    pub async fn surface_replay_inbox(
        &self,
        id: &str,
        message_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/surfaces/{}/inbox/{}/replay",
                url_encode(id),
                url_encode(message_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_retry_outbox(
        &self,
        id: &str,
        delivery_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/surfaces/{}/outbox/{}/retry",
                url_encode(id),
                url_encode(delivery_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_dead_letter_outbox(
        &self,
        id: &str,
        delivery_id: &str,
        reason: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/surfaces/{}/outbox/{}/dead-letter",
                url_encode(id),
                url_encode(delivery_id)
            ),
            serde_json::json!({ "reason": reason }),
        )
        .await
    }

    pub async fn surface_send(
        &self,
        id: &str,
        recipient: &str,
        thread: Option<&str>,
        text: &str,
        metadata: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/send", url_encode(id)),
            serde_json::json!({
                "recipient": recipient,
                "thread": thread,
                "text": text,
                "metadata": metadata,
            }),
        )
        .await
    }

    pub async fn surface_action(
        &self,
        id: &str,
        action: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/action", url_encode(id)),
            serde_json::json!({
                "action": action,
                "payload": payload,
            }),
        )
        .await
    }

    pub async fn skill_runs(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/skills/runs").await
    }

    pub async fn skill_projection(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/skills/projection?surface=tui").await
    }

    pub async fn skill_run_detail(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/skills/runs/{}", url_encode(id)))
            .await
    }

    pub async fn skill_action(
        &self,
        id: &str,
        action: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/skills/{}/actions/{}",
                url_encode(id),
                url_encode(action)
            ),
            payload,
        )
        .await
    }

    pub async fn harness_eval_latest_report(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/harness-eval/reports/latest").await
    }

    pub async fn harness_eval_reports(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/harness-eval/reports").await
    }

    pub async fn harness_eval_report(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/harness-eval/reports/{}", url_encode(id)))
            .await
    }

    pub async fn harness_eval_report_artifacts(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/harness-eval/reports/{}/artifacts",
            url_encode(id)
        ))
        .await
    }

    pub async fn harness_eval_report_gate(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/harness-eval/reports/{}/gate",
            url_encode(id)
        ))
        .await
    }

    pub async fn harness_eval_run_status(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/harness-eval/runs/{}", url_encode(id)))
            .await
    }

    pub async fn harness_eval_run_smoke(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.harness_eval_run(
            "quick",
            "low",
            false,
            "operator requested harness eval smoke",
        )
        .await
    }

    pub async fn harness_eval_run(
        &self,
        level: &str,
        budget: &str,
        allow_real_model: bool,
        objective: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/harness-eval/runs",
            serde_json::json!({
                "level": level,
                "budget": budget,
                "actor": "tui.gateway_panel",
                "objective": objective,
                "allow_real_model": allow_real_model
            }),
        )
        .await
    }

    pub async fn harness_eval_cancel_run(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/harness-eval/runs/{}/cancel", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn evolution_signals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/signals").await
    }

    pub async fn evolution_diagnoses(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/diagnoses").await
    }

    pub async fn evolution_missions_summary(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/missions/summary").await
    }

    pub async fn evolution_mission_detail(
        &self,
        mission_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/evolution/missions/{}/detail",
            url_encode(mission_id)
        ))
        .await
    }

    pub async fn evolution_create_diagnosis(
        &self,
        signal_ids: Vec<String>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/evolution/diagnoses",
            serde_json::json!({ "signal_ids": signal_ids }),
        )
        .await
    }

    pub async fn evolution_proposals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/proposals").await
    }

    pub async fn evolution_create_proposal(
        &self,
        signal_ids: Vec<String>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/evolution/proposals",
            serde_json::json!({ "signal_ids": signal_ids }),
        )
        .await
    }

    pub async fn evolution_skill_draft(
        &self,
        proposal_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/evolution/proposals/{}/skill-draft",
            url_encode(proposal_id)
        ))
        .await
    }

    pub async fn evolution_chain(
        &self,
        proposal_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/evolution/chain/{}", url_encode(proposal_id)))
            .await
    }

    pub async fn evolution_candidates(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/candidates").await
    }

    pub async fn evolution_candidate_detail(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/evolution/candidates/{}",
            url_encode(candidate_id)
        ))
        .await
    }

    pub async fn evolution_create_candidate(
        &self,
        registration: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/evolution/candidates", registration)
            .await
    }

    pub async fn evolution_candidate_canary_review(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/evolution/candidates/{}/reviews/canary",
                url_encode(candidate_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    /// Ask Runtime's trusted evaluator to evaluate one immutable candidate.
    /// This endpoint never accepts a caller-supplied verdict or report.
    pub async fn evolution_candidate_evaluate(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/evolution/candidates/{}/evaluate",
                url_encode(candidate_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn evolution_candidate_stable_review(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/evolution/candidates/{}/reviews/stable",
                url_encode(candidate_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn evolution_reviews(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/reviews").await
    }

    pub async fn evolution_review_detail(
        &self,
        review_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/evolution/reviews/{}", url_encode(review_id)))
            .await
    }

    /// Queue pointer, rollback, or stop-Canary change through Runtime's
    /// typed review gate. TUI cannot mutate a release directly.
    pub async fn evolution_create_release_review(
        &self,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/evolution/reviews", request).await
    }

    pub async fn evolution_review_decision(
        &self,
        review_id: &str,
        decision: &str,
        reason: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/evolution/reviews/{}/decision", url_encode(review_id)),
            serde_json::json!({ "decision": decision, "reason": reason }),
        )
        .await
    }

    /// Read Runtime's protected evaluation-policy floor. The terminal never
    /// computes a release verdict or keeps a policy cache of its own.
    pub async fn evolution_evaluation_policy(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/evaluation-policy").await
    }

    pub async fn evolution_evaluation_policy_reviews(
        &self,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/evaluation-policy/reviews")
            .await
    }

    pub async fn evolution_evaluation_policy_review_decision(
        &self,
        review_id: &str,
        decision: &str,
        reason: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/evolution/evaluation-policy/reviews/{}/decision",
                url_encode(review_id)
            ),
            serde_json::json!({ "decision": decision, "reason": reason }),
        )
        .await
    }

    /// Runtime-owned Managed Agent projection. This is deliberately a single
    /// aggregate read so TUI cannot stitch a second scheduler state together.
    pub async fn managed_agents(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/managed-agents").await
    }

    pub async fn dispatch_managed_agents(
        &self,
        dispatcher_id: &str,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/runtime/managed-agents/dispatch",
            serde_json::json!({ "dispatcher_id": dispatcher_id, "limit": limit }),
        )
        .await
    }

    pub async fn trigger_managed_agent(
        &self,
        managed_agent_id: &str,
        request_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/runtime/managed-agents/{}/trigger",
                url_encode(managed_agent_id)
            ),
            serde_json::json!({ "request_id": request_id }),
        )
        .await
    }

    pub async fn reset_managed_agent_health(
        &self,
        managed_agent_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/runtime/managed-agents/{}/health/reset",
                url_encode(managed_agent_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn connector_service_tools(
        &self,
        service: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/connectors/services/{}/tools",
            url_encode(service)
        ))
        .await
    }

    pub async fn execute_connector_service(
        &self,
        service: &str,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/connectors/services/{}/execute", url_encode(service)),
            request,
        )
        .await
    }

    pub async fn revalidate_connector_resource(
        &self,
        reference: &str,
        state: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/connectors/resources/revalidate",
            serde_json::json!({
                "reference": reference,
                "state": state,
            }),
        )
        .await
    }

    pub async fn promote_connector_resource_to_memory(
        &self,
        reference: &str,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/connectors/resources/promote-memory",
            serde_json::json!({
                "reference": reference,
                "session_id": session_id,
            }),
        )
        .await
    }

    pub async fn preflight_cross_plane_action(
        &self,
        action: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/cross-plane/action/preflight", action)
            .await
    }

    pub async fn execute_cross_plane_action(
        &self,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/cross-plane/action/execute", request)
            .await
    }

    pub async fn cross_plane_policy_simulate(
        &self,
        action: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/cross-plane/policy/simulate", action)
            .await
    }

    pub async fn tool_registry(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/tools").await
    }

    pub async fn tool_execute(
        &self,
        name: &str,
        input: serde_json::Value,
        mode: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/execute",
            serde_json::json!({
                "name": name,
                "input": input,
                "mode": mode,
            }),
        )
        .await
    }

    pub async fn tool_cache_stats(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/tools/cache").await
    }

    pub async fn tool_batch_readonly(
        &self,
        calls: Vec<serde_json::Value>,
        max_concurrency: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/batch-readonly",
            serde_json::json!({
                "calls": calls,
                "max_concurrency": max_concurrency,
            }),
        )
        .await
    }

    pub async fn tool_mutation_preview(
        &self,
        edits: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/mutations/preview",
            serde_json::json!({ "edits": edits }),
        )
        .await
    }

    pub async fn tool_mutation_apply(
        &self,
        edits: Vec<serde_json::Value>,
        expected_hashes: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/mutations/apply",
            serde_json::json!({
                "edits": edits,
                "expected_hashes": expected_hashes,
            }),
        )
        .await
    }

    pub async fn tool_checkpoints(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/tools/checkpoints").await
    }

    pub async fn tool_checkpoint_create(
        &self,
        label: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/checkpoints",
            serde_json::json!({ "label": label }),
        )
        .await
    }

    pub async fn tool_checkpoint_diff(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/tools/checkpoints/{}/diff", url_encode(id)))
            .await
    }

    pub async fn tool_checkpoint_restore(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/tools/checkpoints/{}/restore", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn tool_intent_plan(
        &self,
        prompt: &str,
        selected_tools: Vec<String>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/intent-plan",
            serde_json::json!({
                "prompt": prompt,
                "selected_tools": selected_tools,
            }),
        )
        .await
    }

    pub async fn tool_context_fanout_plan(
        &self,
        prompt: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/context-fanout/plan",
            serde_json::json!({ "prompt": prompt }),
        )
        .await
    }

    /// Execute an APP-selected JSON request through Cowd-owned credentials.
    ///
    /// The external panel can select only an in-process Gateway path and
    /// non-reserved metadata. It cannot override the terminal surface,
    /// observer identity, authentication or HTTP framing.
    pub(crate) async fn app_json_request(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
        headers: &BTreeMap<String, String>,
    ) -> Result<(u16, serde_json::Value), AppTransportFailure> {
        let method = app_method(method)?;
        validate_app_path(path)?;
        let headers = app_headers(headers)?;
        let mut request = self.authorize(
            self.client
                .request(method, format!("{}{}", self.base_url, path)),
        );
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(app_transport_failure)?;
        let status = response.status();
        let bytes = response.bytes().await.map_err(app_transport_failure)?;
        let body = decode_app_json_or_text(&bytes);
        if !status.is_success() {
            return Err(AppTransportFailure {
                status: Some(status.as_u16()),
                body: Some(body.clone()),
                message: format!("Gateway API returned {status}: {body}"),
            });
        }
        Ok((status.as_u16(), body))
    }

    /// Consume a generic APP SSE stream until the host cancels it. Every
    /// decoded data frame is routed back to its originating panel; event
    /// names ending in `error` become a transport failure envelope without
    /// exposing an APP-specific protocol to Cowd.
    pub(crate) async fn subscribe_app_events(
        &self,
        panel_id: String,
        subscription_id: String,
        path: &str,
        headers: &BTreeMap<String, String>,
        mut cancel: watch::Receiver<bool>,
        tx: CowdEventSender,
        session_id: String,
        authority_generation: u64,
    ) -> Result<(), AppTransportFailure> {
        validate_app_path(path)?;
        let headers = app_headers(headers)?;
        let mut request = self.authorize(
            self.sse_client
                .get(format!("{}{}", self.base_url, path))
                .header("Accept", "text/event-stream"),
        );
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request.send().await.map_err(app_transport_failure)?;
        let status = response.status();
        if !status.is_success() {
            let bytes = response.bytes().await.map_err(app_transport_failure)?;
            let body = decode_app_json_or_text(&bytes);
            return Err(AppTransportFailure {
                status: Some(status.as_u16()),
                body: Some(body.clone()),
                message: format!("Gateway SSE returned {status}: {body}"),
            });
        }

        let mut buffer = Vec::new();
        let mut stream = response.bytes_stream();
        loop {
            tokio::select! {
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        return Ok(());
                    }
                }
                chunk = stream.next() => {
                    let Some(chunk) = chunk else {
                        tx.send_wait(session_scoped_event(
                            &session_id,
                            authority_generation,
                            CowdEvent::AppTui {
                                panel_id,
                                event: TuiAppEvent::LiveStopped { subscription_id },
                            },
                        )).await.map_err(|_| AppTransportFailure {
                            status: None,
                            body: None,
                            message: "TUI event receiver stopped while APP SSE ended".to_string(),
                        })?;
                        return Ok(());
                    };
                    let chunk = chunk.map_err(app_transport_failure)?;
                    buffer.extend_from_slice(&chunk);
                    while let Some(frame) = take_gateway_sse_frame(&mut buffer).map_err(app_transport_failure)? {
                        let event_name = gateway_sse_frame_event_name(&frame).unwrap_or_default();
                        let Some(data) = gateway_sse_frame_data(&frame) else {
                            continue;
                        };
                        let parsed = serde_json::from_str::<serde_json::Value>(&data);
                        let event = match parsed {
                            Ok(body) if event_name.ends_with("error") => TuiAppEvent::LiveFailed {
                                subscription_id: subscription_id.clone(),
                                status: None,
                                body: Some(body),
                                error: format!("APP SSE emitted {event_name}"),
                            },
                            Ok(body) => TuiAppEvent::LiveEnvelope {
                                subscription_id: subscription_id.clone(),
                                body,
                            },
                            Err(error) => TuiAppEvent::LiveFailed {
                                subscription_id: subscription_id.clone(),
                                status: None,
                                body: None,
                                error: format!("APP SSE payload is not valid JSON: {error}"),
                            },
                        };
                        tx.send_wait(session_scoped_event(
                            &session_id,
                            authority_generation,
                            CowdEvent::AppTui {
                                panel_id: panel_id.clone(),
                                event,
                            },
                        )).await.map_err(|_| AppTransportFailure {
                            status: None,
                            body: None,
                            message: "TUI event receiver stopped while APP SSE was active".to_string(),
                        })?;
                    }
                }
            }
        }
    }

    async fn get_json(&self, path: &str) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.get(url));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        response.json().await.map_err(GatewayApiError::Http)
    }

    async fn post_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.post(url).json(&body));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        response.json().await.map_err(GatewayApiError::Http)
    }

    async fn put_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.put(url).json(&body));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        response.json().await.map_err(GatewayApiError::Http)
    }

    async fn patch_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.patch(url).json(&body));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        response.json().await.map_err(GatewayApiError::Http)
    }

    async fn delete_json(&self, path: &str) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.delete(url));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        let body = response.text().await.map_err(GatewayApiError::Http)?;
        if body.trim().is_empty() {
            Ok(serde_json::json!({ "ok": true }))
        } else {
            serde_json::from_str(&body).map_err(|error| GatewayApiError::Url(error.to_string()))
        }
    }

    async fn get_bytes(&self, path: &str) -> Result<Vec<u8>, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.get(url));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(GatewayApiError::Http)
    }
}

fn take_gateway_sse_frame(buffer: &mut Vec<u8>) -> Result<Option<String>, GatewayApiError> {
    const MAX_GATEWAY_SSE_FRAME_BYTES: usize = 2 * 1024 * 1024;
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    let Some((index, delimiter_len)) = (match (lf, crlf) {
        (Some(lf), Some(crlf)) if lf <= crlf => Some((lf, 2)),
        (Some(_), Some(crlf)) => Some((crlf, 4)),
        (Some(lf), None) => Some((lf, 2)),
        (None, Some(crlf)) => Some((crlf, 4)),
        (None, None) => None,
    }) else {
        if buffer.len() > MAX_GATEWAY_SSE_FRAME_BYTES {
            return Err(GatewayApiError::Url(
                "Gateway SSE frame exceeded the 2 MiB transport limit".to_string(),
            ));
        }
        return Ok(None);
    };
    if index > MAX_GATEWAY_SSE_FRAME_BYTES {
        return Err(GatewayApiError::Url(
            "Gateway SSE frame exceeded the 2 MiB transport limit".to_string(),
        ));
    }
    let frame = buffer[..index].to_vec();
    buffer.drain(..index + delimiter_len);
    String::from_utf8(frame)
        .map(Some)
        .map_err(|_| GatewayApiError::Url("Gateway SSE stream emitted invalid UTF-8".to_string()))
}

fn app_method(method: &str) -> Result<reqwest::Method, AppTransportFailure> {
    let method = method.trim().to_ascii_uppercase();
    reqwest::Method::from_bytes(method.as_bytes()).map_err(|_| AppTransportFailure {
        status: None,
        body: None,
        message: format!("APP request method is invalid: {method}"),
    })
}

fn validate_app_path(path: &str) -> Result<(), AppTransportFailure> {
    let path_without_query = path.split_once('?').map_or(path, |(path, _)| path);
    let percent_lower = path_without_query.to_ascii_lowercase();
    let traversal = path_without_query
        .split('/')
        .any(|segment| matches!(segment, "." | ".."))
        || percent_lower.contains("%2e");
    if path_without_query.starts_with("/api/")
        && !path.contains("://")
        && !path.contains('\\')
        && !path.contains('\r')
        && !path.contains('\n')
        && !path.contains('#')
        && !traversal
    {
        return Ok(());
    }
    Err(AppTransportFailure {
        status: None,
        body: None,
        message: "APP request path must be a Gateway-local /api/ path".to_string(),
    })
}

fn app_headers(
    headers: &BTreeMap<String, String>,
) -> Result<Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>, AppTransportFailure> {
    const RESERVED: &[&str] = &[
        "authorization",
        "cookie",
        "host",
        "content-length",
        "content-type",
        "accept",
        "x-cowd-surface-id",
        "x-cowd-observer-id",
    ];
    if headers.len() > 32 {
        return Err(AppTransportFailure {
            status: None,
            body: None,
            message: "APP request supplied too many headers".to_string(),
        });
    }
    headers
        .iter()
        .map(|(name, value)| {
            let normalized = name.trim().to_ascii_lowercase();
            if RESERVED.contains(&normalized.as_str()) {
                return Err(AppTransportFailure {
                    status: None,
                    body: None,
                    message: format!(
                        "APP request attempted to override reserved header {normalized}"
                    ),
                });
            }
            let name =
                reqwest::header::HeaderName::from_bytes(normalized.as_bytes()).map_err(|_| {
                    AppTransportFailure {
                        status: None,
                        body: None,
                        message: format!("APP request header is invalid: {normalized}"),
                    }
                })?;
            let value =
                reqwest::header::HeaderValue::from_str(value).map_err(|_| AppTransportFailure {
                    status: None,
                    body: None,
                    message: format!("APP request header value is invalid: {normalized}"),
                })?;
            Ok((name, value))
        })
        .collect()
}

fn decode_app_json_or_text(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(bytes).to_string()))
}

fn app_transport_failure(error: impl fmt::Display) -> AppTransportFailure {
    AppTransportFailure {
        status: None,
        body: None,
        message: error.to_string(),
    }
}

fn gateway_listener_reachable(base_url: &str) -> bool {
    let normalized = match normalize_base_url(base_url.to_string()) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let without_scheme = normalized
        .strip_prefix("http://")
        .or_else(|| normalized.strip_prefix("https://"))
        .unwrap_or(&normalized);
    let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);
    let Ok(addrs) = host_port.to_socket_addrs() else {
        return false;
    };
    addrs
        .into_iter()
        .any(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok())
}

pub fn default_auth_token() -> Option<String> {
    std::env::var("COWD_API_TOKEN")
        .ok()
        .or_else(|| std::env::var("COWD_AUTH_TOKEN").ok())
        .or_else(|| {
            let home = std::env::var("HOME").ok()?;
            let config_path = std::path::PathBuf::from(home)
                .join(".cowd")
                .join("config.yaml");
            let config = std::fs::read_to_string(&config_path).ok()?;
            for line in config.lines() {
                let trimmed = line.trim();
                if trimmed.starts_with("token:") {
                    let token = trimmed.strip_prefix("token:")?.trim().trim_matches('"');
                    if !token.is_empty() {
                        return Some(token.to_string());
                    }
                }
            }
            None
        })
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
}

fn normalize_base_url(mut base_url: String) -> Result<String, GatewayApiError> {
    if base_url.trim().is_empty() {
        return Err(GatewayApiError::Url(
            "empty Gateway API base URL".to_string(),
        ));
    }
    base_url = base_url.trim().trim_end_matches('/').to_string();
    if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
        return Err(GatewayApiError::Url(format!(
            "Gateway API base URL must start with http:// or https://: {base_url}"
        )));
    }
    Ok(base_url)
}

fn url_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            _ => format!("%{b:02X}").chars().collect(),
        })
        .collect()
}

fn gateway_status_error(status: reqwest::StatusCode, body: String) -> GatewayApiError {
    GatewayApiError::Status(status, body)
}

impl fmt::Display for GatewayApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(err) => write!(f, "Gateway API HTTP failed: {err}"),
            Self::Status(status, body) => {
                write!(f, "Gateway API returned {status}: {body}")
            }
            Self::SessionAuthorizationRevoked(reason) => {
                write!(f, "Gateway revoked session authorization: {reason}")
            }
            Self::Contract(err) => write!(f, "Gateway projection contract error: {err}"),
            Self::Url(err) => write!(f, "Gateway API URL error: {err}"),
        }
    }
}

impl std::error::Error for GatewayApiError {}

fn require_gateway_operation_ok(
    value: serde_json::Value,
    operation: &str,
) -> Result<serde_json::Value, GatewayApiError> {
    if value.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
        return Ok(value);
    }
    let reason = value
        .get("error")
        .and_then(serde_json::Value::as_str)
        .filter(|reason| !reason.trim().is_empty())
        .unwrap_or("Gateway returned an unsuccessful operation receipt");
    Err(GatewayApiError::Url(format!(
        "{operation} failed: {reason}"
    )))
}

fn require_gateway_session_operation_ok(
    value: serde_json::Value,
    operation: &str,
    requested_session_id: &str,
) -> Result<serde_json::Value, GatewayApiError> {
    let value = require_gateway_operation_ok(value, operation)?;
    validate_session_json_identity(
        requested_session_id,
        &value,
        &format!("{operation} receipt"),
    )?;
    Ok(value)
}

fn string_field(value: &serde_json::Value, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn gateway_event_correlation(
    value: &serde_json::Value,
    session_id: Option<&str>,
    part_id: Option<String>,
) -> GatewayEventCorrelation {
    GatewayEventCorrelation {
        session_id: string_field(value, "session_id")
            .or_else(|| session_id.map(ToOwned::to_owned))
            .unwrap_or_default(),
        execution_id: string_field(value, "execution_id"),
        turn_id: string_field(value, "turn_id"),
        part_id: string_field(value, "part_id").or(part_id),
        model_step_id: string_field(value, "model_step_id"),
        item_id: string_field(value, "item_id"),
        segment_id: string_field(value, "segment_id"),
        tool_call_id: string_field(value, "tool_call_id"),
        causal_sequence: value
            .get("causal_sequence")
            .and_then(serde_json::Value::as_u64),
        delta_sequence: value
            .get("delta_sequence")
            .and_then(serde_json::Value::as_u64),
        causal_parent_ids: value
            .get("causal_parent_ids")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .collect(),
        message_id: string_field(value, "message_id"),
        terminal_id: string_field(value, "terminal_id"),
        commit_cursor: value
            .get("runtime_commit_cursor")
            .and_then(serde_json::Value::as_u64),
        replayed: value
            .get("replayed")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    }
}

fn execution_live_status(
    value: &serde_json::Value,
) -> Option<harness_contract::projection::ExecutionLiveStatus> {
    let normalized = value.as_str()?.trim().to_ascii_lowercase();
    use harness_contract::projection::ExecutionLiveStatus;
    match normalized.as_str() {
        "queued" => Some(ExecutionLiveStatus::Queued),
        "preparingcontext" | "preparing_context" => Some(ExecutionLiveStatus::PreparingContext),
        "callingmodel" | "calling_model" => Some(ExecutionLiveStatus::CallingModel),
        "thinking" => Some(ExecutionLiveStatus::Thinking),
        "callingtool" | "calling_tool" => Some(ExecutionLiveStatus::CallingTool),
        "waitingapproval" | "waiting_approval" => Some(ExecutionLiveStatus::WaitingApproval),
        "finalizing" => Some(ExecutionLiveStatus::Finalizing),
        "complete" | "completed" => Some(ExecutionLiveStatus::Complete),
        "cancelled" | "canceled" => Some(ExecutionLiveStatus::Cancelled),
        "error" | "failed" => Some(ExecutionLiveStatus::Error),
        _ => None,
    }
}

async fn hydrate_session_history_with_retry(
    client: &GatewayApiClient,
    session_id: &str,
    tx: CowdEventSender,
    next_sequence: Arc<AtomicUsize>,
    authority_generation: u64,
) {
    let mut retry_delay = Duration::from_millis(250);
    let mut attempt = 0u32;
    loop {
        let from_message_sequence = next_sequence.load(Ordering::Acquire);
        match hydrate_session_history_once(
            client,
            session_id,
            tx.clone(),
            from_message_sequence,
            &next_sequence,
            authority_generation,
        )
        .await
        {
            Ok(hydrated_to) => {
                next_sequence.fetch_max(hydrated_to, Ordering::AcqRel);
                return;
            }
            Err(error) => {
                attempt = attempt.saturating_add(1);
                if attempt == 1 || attempt % 12 == 0 {
                    let _ = tx.send(session_scoped_event(
                        session_id,
                        authority_generation,
                        CowdEvent::SessionHistoryHydrationFailed {
                            session_id: session_id.to_string(),
                            error: format!("{error}; retry attempt {attempt}"),
                        },
                    ));
                }
                tokio::time::sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
            }
        }
    }
}

fn session_scoped_event(
    session_id: &str,
    authority_generation: u64,
    event: CowdEvent,
) -> CowdEvent {
    CowdEvent::SessionScoped {
        session_id: session_id.to_string(),
        authority_generation,
        event: Box::new(event),
    }
}

async fn deliver_session_stream_event(
    tx: &CowdEventSender,
    session_id: &str,
    event: CowdEvent,
    authority_generation: u64,
) -> Result<(), GatewayApiError> {
    let event = session_scoped_event(session_id, authority_generation, event);
    // The shared live multiplexer has already applied the delivery-class
    // policy: ephemeral previews may be coalesced there, while durable and
    // reconstructable envelopes are lossless. Once an envelope enters a
    // session bridge, queue pressure must backpressure that bridge instead of
    // turning an otherwise healthy live source into a reconnect. In
    // particular, dropping TextDelta here makes every Surface see only the
    // terminal replay during event bursts.
    tx.send_wait(event)
        .await
        .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))
}

async fn deliver_session_stream_event_with_catchup(
    client: &GatewayApiClient,
    tx: &CowdEventSender,
    session_id: &str,
    event: CowdEvent,
    next_message_sequence: &Arc<AtomicUsize>,
    authority_generation: u64,
) -> Result<(), GatewayApiError> {
    if let CowdEvent::GatewaySession {
        event: GatewaySessionEvent::UserMessageCommitted { sequence, .. },
    } = &event
    {
        next_message_sequence.fetch_max(sequence.saturating_add(1), Ordering::AcqRel);
    }
    let terminal = matches!(
        &event,
        CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TerminalCommitted { .. }
        }
    );
    deliver_session_stream_event(tx, session_id, event, authority_generation).await?;
    if terminal {
        let catchup_from = next_message_sequence.load(Ordering::Acquire);
        if let Err(error) = hydrate_session_history_once(
            client,
            session_id,
            tx.clone(),
            catchup_from,
            next_message_sequence.as_ref(),
            authority_generation,
        )
        .await
        {
            let _ = tx.send(session_scoped_event(
                session_id,
                authority_generation,
                CowdEvent::Warning {
                    message: format!(
                        "Terminal committed, but durable transcript catch-up failed: {error}"
                    ),
                },
            ));
        }
    }
    Ok(())
}

async fn hydrate_session_history_once(
    client: &GatewayApiClient,
    session_id: &str,
    tx: CowdEventSender,
    mut from_sequence: usize,
    accepted_sequence: &AtomicUsize,
    authority_generation: u64,
) -> Result<usize, GatewayApiError> {
    const HISTORY_WINDOW_CAP: usize = 500;
    let started = std::time::Instant::now();
    let hydration_kind = if from_sequence == 0 {
        crate::protocol::SessionHistoryHydrationKind::InitialWindow
    } else {
        crate::protocol::SessionHistoryHydrationKind::IncrementalCatchup
    };
    let mut message_count = 0usize;
    let mut page_count = 0usize;
    let mut oldest_offset = 0usize;
    let mut total_messages = 0usize;
    let mut has_older = false;

    if from_sequence == 0 {
        // Materialize the body-free index first, then fetch only the bounded
        // transcript tail. Older bodies remain available through explicit
        // paging and exact reads.
        let history_index = client.session_history_index(session_id).await?;
        total_messages = history_index.total_messages as usize;
        tx.send_wait(session_scoped_event(
            session_id,
            authority_generation,
            CowdEvent::SessionHistoryIndexLoaded {
                projection: history_index,
            },
        ))
        .await
        .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))?;
        oldest_offset = total_messages.saturating_sub(HISTORY_WINDOW_CAP);
        has_older = oldest_offset > 0;
        let mut offset = oldest_offset;
        loop {
            let page = client
                .session_messages_offset(session_id, offset, 500)
                .await?;
            let loaded = page.messages.len();
            let next_sequence = page.next_seq;
            let page_has_more = page.has_more;
            total_messages = total_messages.max(page.total);
            message_count = message_count.saturating_add(loaded);
            page_count = page_count.saturating_add(1);
            tx.send_wait(session_scoped_event(
                session_id,
                authority_generation,
                CowdEvent::SessionHistoryPage { page },
            ))
            .await
            .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))?;
            if let Some(next) = next_sequence {
                from_sequence = from_sequence.max(next);
                accepted_sequence.fetch_max(from_sequence, Ordering::AcqRel);
            }
            if !page_has_more {
                break;
            }
            if loaded == 0 {
                return Err(GatewayApiError::Url(
                    "Gateway returned a non-advancing history offset".to_string(),
                ));
            }
            offset = offset.saturating_add(loaded);
        }
    } else {
        // Reconnect hydration starts at the last accepted durable sequence and
        // fetches only messages committed while the Surface was away.
        loop {
            let page = client
                .session_messages(session_id, from_sequence, 500)
                .await?;
            let next_sequence = page.next_seq;
            let has_more = page.has_more;
            total_messages = total_messages.max(page.total);
            message_count = message_count.saturating_add(page.messages.len());
            page_count = page_count.saturating_add(1);
            tx.send_wait(session_scoped_event(
                session_id,
                authority_generation,
                CowdEvent::SessionHistoryCatchupPage { page },
            ))
            .await
            .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))?;
            if let Some(next) = next_sequence {
                accepted_sequence.fetch_max(next, Ordering::AcqRel);
            }
            if !has_more {
                from_sequence = next_sequence.unwrap_or(from_sequence);
                break;
            }
            let Some(next_sequence) = next_sequence.filter(|next| *next > from_sequence) else {
                return Err(GatewayApiError::Url(
                    "Gateway returned a non-advancing history cursor".to_string(),
                ));
            };
            from_sequence = next_sequence;
        }
    }
    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    tx.send_wait(session_scoped_event(
        session_id,
        authority_generation,
        CowdEvent::SessionHistoryHydrated {
            session_id: session_id.to_string(),
            kind: hydration_kind,
            duration_ms,
            message_count,
            page_count,
            oldest_offset,
            total_messages,
            next_sequence: from_sequence,
            has_older,
        },
    ))
    .await
    .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))?;
    tracing::info!(
        session_id,
        duration_ms,
        message_count,
        page_count,
        oldest_offset,
        total_messages,
        has_older,
        "TUI durable history hydration completed"
    );
    Ok(from_sequence)
}

pub(crate) fn gateway_sse_json_to_cowd_event_for_session(
    value: &serde_json::Value,
    session_id: Option<&str>,
) -> Option<CowdEvent> {
    let event_type = value
        .get("type")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("event_type"))
        .and_then(serde_json::Value::as_str)?;
    match event_type {
        "UserMessageCommitted" | "user_message_committed" => {
            let correlation = gateway_event_correlation(value, session_id, None);
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::UserMessageCommitted {
                    correlation,
                    content: value
                        .get("content")
                        .and_then(serde_json::Value::as_str)?
                        .to_string(),
                    sequence: value.get("sequence").and_then(serde_json::Value::as_u64)? as usize,
                    created_at_ms: value
                        .get("created_at_ms")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default(),
                },
            })
        }
        "TextDelta" | "text_delta" | "assistant_delta" => {
            let text = value
                .get("text")
                .or_else(|| value.get("content"))
                .and_then(serde_json::Value::as_str)?
                .to_string();
            let correlation = gateway_event_correlation(value, session_id, None);
            let start_bytes = value.get("start_bytes")?.as_u64()? as usize;
            let end_bytes = value.get("end_bytes")?.as_u64()? as usize;
            let stream_revision = value.get("stream_revision")?.as_u64()?;
            if end_bytes < start_bytes || end_bytes.saturating_sub(start_bytes) != text.len() {
                return None;
            }
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::TextDelta {
                    correlation,
                    text,
                    start_bytes,
                    end_bytes,
                    stream_revision,
                },
            })
        }
        "ReasoningSummaryDelta" | "reasoning_summary_delta" => {
            let summary = value
                .get("summary")
                .or_else(|| value.get("content"))
                .and_then(serde_json::Value::as_str)?
                .to_string();
            let correlation = gateway_event_correlation(value, session_id, None);
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ReasoningSummaryDelta {
                    correlation,
                    summary,
                },
            })
        }
        "ModelStepStarted" | "model_step_started" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ModelStepStarted {
                correlation: gateway_event_correlation(value, session_id, None),
            },
        }),
        "ModelStepCompleted" | "model_step_completed" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ModelStepCompleted {
                correlation: gateway_event_correlation(value, session_id, None),
                status: value
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("complete")
                    .to_string(),
            },
        }),
        "ItemStarted" | "item_started" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ItemStarted {
                correlation: gateway_event_correlation(value, session_id, None),
                kind: value
                    .get("kind")
                    .and_then(serde_json::Value::as_str)?
                    .to_string(),
            },
        }),
        "ItemCompleted" | "item_completed" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ItemCompleted {
                correlation: gateway_event_correlation(value, session_id, None),
                kind: value
                    .get("kind")
                    .and_then(serde_json::Value::as_str)?
                    .to_string(),
            },
        }),
        "ToolStart" | "tool_start" => {
            let id = value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.trim().is_empty())?
                .to_string();
            let correlation = gateway_event_correlation(value, session_id, Some(id.clone()));
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ToolStart {
                    correlation,
                    id,
                    name: value
                        .get("name")
                        .or_else(|| value.get("tool_name"))
                        .and_then(serde_json::Value::as_str)
                        .filter(|name| !name.trim().is_empty())?
                        .to_string(),
                    preview: value
                        .get("preview")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                },
            })
        }
        "ToolProgress" | "tool_progress" => {
            let id = value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.trim().is_empty())?
                .to_string();
            let correlation = gateway_event_correlation(value, session_id, Some(id.clone()));
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ToolProgress {
                    correlation,
                    id,
                    name: value
                        .get("name")
                        .or_else(|| value.get("tool_name"))
                        .and_then(serde_json::Value::as_str)
                        .filter(|name| !name.trim().is_empty())?
                        .to_string(),
                    progress: value
                        .get("progress")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                },
            })
        }
        "ToolComplete" | "tool_complete" => {
            let id = value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.trim().is_empty())?
                .to_string();
            let correlation = gateway_event_correlation(value, session_id, Some(id.clone()));
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ToolComplete {
                    correlation,
                    id,
                    name: value
                        .get("name")
                        .or_else(|| value.get("tool_name"))
                        .and_then(serde_json::Value::as_str)
                        .filter(|name| !name.trim().is_empty())?
                        .to_string(),
                    summary: value
                        .get("result_summary")
                        .or_else(|| value.get("summary"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    exit_code: value
                        .get("exit_code")
                        .and_then(serde_json::Value::as_i64)
                        .map(|code| code as i32),
                },
            })
        }
        // A model loop completion is only rendering progress. The durable
        // SessionRuntimeBridge emits TerminalCommitted after the transcript
        // write succeeds; only that event is allowed to settle TUI state.
        "TurnComplete" | "turn_complete" => None,
        "TerminalCommitted" | "terminal_committed" => {
            let correlation = gateway_event_correlation(value, session_id, None);
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::TerminalCommitted {
                    correlation,
                    assistant_text: value
                        .get("assistant_text")
                        .or_else(|| value.get("response"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    sequence: value
                        .get("sequence")
                        .and_then(serde_json::Value::as_u64)
                        .and_then(|value| usize::try_from(value).ok()),
                    iterations: value
                        .get("iterations")
                        .and_then(serde_json::Value::as_u64)
                        .map(|value| value as u32)
                        .unwrap_or_default(),
                    token_usage: value.get("token_usage").cloned(),
                },
            })
        }
        "TurnError" | "turn_error" => {
            let correlation = gateway_event_correlation(value, session_id, None);
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::TurnError {
                    correlation,
                    error: value
                        .get("error")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("Gateway turn error")
                        .to_string(),
                },
            })
        }
        "ExecutionPhase" | "execution_phase" => {
            let status = execution_live_status(value.get("status")?)?;
            let detail = value
                .get("detail")
                .and_then(serde_json::Value::as_str)
                .filter(|detail| !detail.trim().is_empty())
                .map(ToOwned::to_owned);
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ExecutionPhase {
                    correlation: gateway_event_correlation(value, session_id, None),
                    status,
                    detail,
                },
            })
        }
        "SessionInputReceived" | "session_input_received" => {
            if let Some(projection) = value.get("input_projection") {
                return Some(CowdEvent::SessionInputProjection {
                    projection: projection.clone(),
                });
            }
            let decision = value
                .get("receipt")
                .or_else(|| value.get("input"))
                .and_then(|receipt| receipt.get("decision"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("received");
            Some(CowdEvent::Warning {
                message: format!("Session input received: {decision}"),
            })
        }
        "SessionInputProjection" | "session_input_projection" => value
            .get("projection")
            .cloned()
            .map(|projection| CowdEvent::SessionInputProjection { projection }),
        "TurnInboxUpdated" | "turn_inbox_updated" => {
            // Older Gateway builds may publish an inbox without the adjacent
            // full projection. Treat its typed items as a bounded projection
            // instead of emitting a notice while leaving stale queue state on
            // screen.
            value.get("inbox").map(|inbox| CowdEvent::SessionInputProjection {
                projection: serde_json::json!({
                    "session_id": inbox.get("session_id").cloned().unwrap_or_default(),
                    "active_turn_id": inbox.get("turn_id").cloned().unwrap_or_default(),
                    "pending_count": inbox.get("pending_count").cloned().unwrap_or_default(),
                    "inputs": inbox.get("items").cloned().unwrap_or_else(|| serde_json::json!([])),
                }),
            })
        }
        "TurnInputCheckpointConsumed" | "turn_input_checkpoint_consumed" => {
            let checkpoint = value
                .get("checkpoint")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("checkpoint");
            let consumed = value
                .get("consumed")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or_default();
            Some(CowdEvent::Warning {
                message: format!("Runtime consumed {consumed} input(s) at {checkpoint}"),
            })
        }
        "TurnCancelRequested" | "turn_cancel_requested" => Some(CowdEvent::Warning {
            message: "Gateway cancel request accepted".to_string(),
        }),
        "ContextEnvelope" | "context_envelope" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ContextEnvelope {
                correlation: gateway_event_correlation(value, session_id, None),
                envelope: value.get("envelope").cloned()?,
            },
        }),
        "TokenUsage" | "token_usage" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TokenUsage {
                correlation: gateway_event_correlation(value, session_id, None),
                input: value.get("input").and_then(serde_json::Value::as_u64)?,
                output: value.get("output").and_then(serde_json::Value::as_u64)?,
                cache_create: value
                    .get("cache_create")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                cache_read: value
                    .get("cache_read")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
            },
        }),
        "RunModelTelemetry" | "run_model_telemetry" => value
            .get("telemetry")
            .cloned()
            .and_then(|telemetry| serde_json::from_value(telemetry).ok())
            .map(|telemetry| CowdEvent::GatewaySession {
                event: GatewaySessionEvent::RunModelTelemetry {
                    correlation: gateway_event_correlation(value, session_id, None),
                    telemetry,
                },
            }),
        "ContextWindow" | "context_window" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ContextWindow {
                correlation: gateway_event_correlation(value, session_id, None),
                value: value.get("value").and_then(serde_json::Value::as_u64)?,
            },
        }),
        "ProviderAttempt" | "provider_attempt" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ProviderAttempt {
                correlation: gateway_event_correlation(value, session_id, None),
                model: value.get("model")?.as_str()?.to_string(),
                models_tried: value
                    .get("models_tried")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect(),
                context_window_tokens: value
                    .get("context_window_tokens")
                    .and_then(serde_json::Value::as_u64)?,
                context_window_source: value
                    .get("context_window_source")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
                    .to_string(),
                packed_input_tokens: value
                    .get("packed_input_tokens")
                    .and_then(serde_json::Value::as_u64)?,
            },
        }),
        "Warning" | "warning" => value
            .get("message")
            .and_then(serde_json::Value::as_str)
            .map(|message| CowdEvent::Warning {
                message: message.to_string(),
            }),
        "CompactionNotice" | "compaction_notice" => value
            .get("removed_count")
            .and_then(serde_json::Value::as_u64)
            .and_then(|count| usize::try_from(count).ok())
            .map(|removed_count| CowdEvent::CompactionNotice { removed_count }),
        "RuntimeEventEncodingError" => Some(CowdEvent::Warning {
            message: format!(
                "Gateway could not encode a Runtime event: {}",
                value
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown encoding failure")
            ),
        }),
        "RuntimePolicyDecision" | "runtime_policy_decision" => value
            .get("summary")
            .cloned()
            .and_then(|summary| serde_json::from_value(summary).ok())
            .map(|summary| CowdEvent::RuntimePolicyDecision { summary }),
        "ExecutionGraphSummary" | "execution_graph_summary" => value
            .get("summary")
            .cloned()
            .and_then(|summary| serde_json::from_value(summary).ok())
            .map(|summary| CowdEvent::ExecutionGraphSummary { summary }),
        _ => None,
    }
}

pub fn gateway_sse_json_to_cowd_event(value: &serde_json::Value) -> Option<CowdEvent> {
    gateway_sse_json_to_cowd_event_for_session(value, None)
}

pub fn gateway_sse_frame_to_cowd_event(frame: &str) -> Option<CowdEvent> {
    gateway_sse_frame_to_cowd_event_for_session(frame, "")
}

fn gateway_sse_frame_to_cowd_event_for_session(frame: &str, session_id: &str) -> Option<CowdEvent> {
    strict_gateway_sse_frame_to_cowd_event_for_session(frame, session_id)
        .ok()
        .flatten()
}

fn strict_gateway_sse_frame_to_cowd_event_for_session(
    frame: &str,
    session_id: &str,
) -> Result<Option<CowdEvent>, String> {
    let data = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if data.is_empty() || data == "[DONE]" {
        return Ok(None);
    }
    let value = serde_json::from_str::<serde_json::Value>(&data)
        .map_err(|error| format!("invalid session event JSON: {error}"))?;
    let event_type = value
        .get("type")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("event_type"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "session event has no typed identity".to_string())?;
    let frame_cursor = gateway_sse_frame_commit_cursor(frame);
    validate_gateway_session_event_contract(&value, event_type, session_id, frame_cursor)?;
    if let Some(mut event) = gateway_sse_json_to_cowd_event_for_session(&value, Some(session_id)) {
        if let CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TerminalCommitted { correlation, .. },
        } = &mut event
        {
            if correlation.commit_cursor.is_none() {
                correlation.commit_cursor = frame_cursor;
            }
        }
        return Ok(Some(event));
    }
    if matches!(
        event_type,
        "Connected"
            | "connected"
            | "TurnComplete"
            | "turn_complete"
            | "TurnStarted"
            | "turn_started"
            | "ToolExecuted"
            | "tool_executed"
            | "WriteAttemptsObserved"
            | "write_attempts_observed"
    ) {
        return Ok(None);
    }
    if matches!(
        event_type,
        "UserMessageCommitted"
            | "user_message_committed"
            | "TextDelta"
            | "text_delta"
            | "assistant_delta"
            | "ReasoningSummaryDelta"
            | "reasoning_summary_delta"
            | "ModelStepStarted"
            | "model_step_started"
            | "ModelStepCompleted"
            | "model_step_completed"
            | "ItemStarted"
            | "item_started"
            | "ItemCompleted"
            | "item_completed"
            | "ToolStart"
            | "tool_start"
            | "ToolProgress"
            | "tool_progress"
            | "ToolComplete"
            | "tool_complete"
            | "TerminalCommitted"
            | "terminal_committed"
            | "TurnError"
            | "turn_error"
            | "ExecutionPhase"
            | "execution_phase"
            | "ContextEnvelope"
            | "context_envelope"
            | "TokenUsage"
            | "token_usage"
            | "RunModelTelemetry"
            | "run_model_telemetry"
            | "ContextWindow"
            | "context_window"
            | "ProviderAttempt"
            | "provider_attempt"
            | "Warning"
            | "warning"
            | "CompactionNotice"
            | "compaction_notice"
            | "RuntimePolicyDecision"
            | "runtime_policy_decision"
            | "ExecutionGraphSummary"
            | "execution_graph_summary"
    ) {
        return Err(format!(
            "recognized session event `{event_type}` is missing or has invalid required fields"
        ));
    }
    Ok(Some(CowdEvent::Warning {
        message: format!(
            "Gateway session stream exposed an unsupported event type `{event_type}`; the event was not applied"
        ),
    }))
}

fn validate_gateway_session_event_contract(
    value: &serde_json::Value,
    event_type: &str,
    subscribed_session_id: &str,
    frame_cursor: Option<u64>,
) -> Result<(), String> {
    if value.get("session_id").is_some() {
        let explicit_session = string_field(value, "session_id").ok_or_else(|| {
            format!("`{event_type}` contains an empty or non-string `session_id`")
        })?;
        if !subscribed_session_id.trim().is_empty() && explicit_session != subscribed_session_id {
            return Err(format!(
                "`{event_type}` session `{explicit_session}` does not match subscribed session `{subscribed_session_id}`"
            ));
        }
    }
    let require_text = |field: &str| {
        string_field(value, field)
            .map(|_| ())
            .ok_or_else(|| format!("`{event_type}` requires non-empty `{field}`"))
    };
    let require_session = || {
        let event_session = string_field(value, "session_id")
            .or_else(|| {
                (!subscribed_session_id.trim().is_empty())
                    .then(|| subscribed_session_id.to_string())
            })
            .ok_or_else(|| format!("`{event_type}` requires non-empty `session_id`"))?;
        if !subscribed_session_id.trim().is_empty() && event_session != subscribed_session_id {
            return Err(format!(
                "`{event_type}` session `{event_session}` does not match subscribed session `{subscribed_session_id}`"
            ));
        }
        Ok::<(), String>(())
    };
    let require_execution = || {
        require_session()?;
        require_text("execution_id")?;
        require_text("turn_id")
    };
    let require_causal_item = || {
        require_execution()?;
        require_text("model_step_id")?;
        require_text("item_id")?;
        require_text("segment_id")?;
        value
            .get("causal_sequence")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| format!("`{event_type}` requires integer `causal_sequence`"))?;
        value
            .get("delta_sequence")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| format!("`{event_type}` requires integer `delta_sequence`"))?;
        Ok::<(), String>(())
    };
    match event_type {
        "UserMessageCommitted" | "user_message_committed" => {
            require_execution()?;
            require_text("message_id")?;
            value
                .get("sequence")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| format!("`{event_type}` requires integer `sequence`"))?;
            require_text("content")
        }
        "TextDelta" | "text_delta" | "assistant_delta" => {
            require_causal_item()?;
            require_text("part_id")?;
            let text = value
                .get("text")
                .or_else(|| value.get("content"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("`{event_type}` requires string `text`"))?;
            let start = value
                .get("start_bytes")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| format!("`{event_type}` requires integer `start_bytes`"))?;
            let end = value
                .get("end_bytes")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| format!("`{event_type}` requires integer `end_bytes`"))?;
            value
                .get("stream_revision")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| format!("`{event_type}` requires integer `stream_revision`"))?;
            if end < start || end.saturating_sub(start) != text.len() as u64 {
                return Err(format!(
                    "`{event_type}` byte range does not match UTF-8 text length"
                ));
            }
            Ok(())
        }
        "ReasoningSummaryDelta" | "reasoning_summary_delta" => {
            require_causal_item()?;
            require_text("part_id")?;
            value
                .get("summary")
                .or_else(|| value.get("content"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("`{event_type}` requires string `summary`"))
                .map(|_| ())
        }
        "ToolStart" | "tool_start" | "ToolProgress" | "tool_progress" | "ToolComplete"
        | "tool_complete" => {
            require_causal_item()?;
            require_text("part_id")?;
            require_text("tool_call_id")?;
            require_text("id")?;
            value
                .get("name")
                .or_else(|| value.get("tool_name"))
                .and_then(serde_json::Value::as_str)
                .filter(|name| !name.trim().is_empty())
                .ok_or_else(|| format!("`{event_type}` requires non-empty `name`"))?;
            Ok(())
        }
        "ModelStepStarted"
        | "model_step_started"
        | "ModelStepCompleted"
        | "model_step_completed" => {
            require_execution()?;
            require_text("model_step_id")
        }
        "ItemStarted" | "item_started" | "ItemCompleted" | "item_completed" => {
            require_causal_item()?;
            require_text("kind")
        }
        "TerminalCommitted" | "terminal_committed" => {
            require_execution()?;
            require_text("part_id")?;
            require_text("message_id")?;
            require_text("terminal_id")?;
            let payload_cursor = value
                .get("runtime_commit_cursor")
                .and_then(serde_json::Value::as_u64);
            if payload_cursor.or(frame_cursor).is_none() {
                return Err(format!(
                    "`{event_type}` requires an integer durable commit cursor"
                ));
            }
            value
                .get("assistant_text")
                .or_else(|| value.get("response"))
                .and_then(serde_json::Value::as_str)
                .filter(|response| !response.trim().is_empty())
                .ok_or_else(|| {
                    format!("`{event_type}` requires non-empty assistant terminal text")
                })?;
            Ok(())
        }
        "ExecutionPhase" | "execution_phase" | "TurnError" | "turn_error" => require_execution(),
        "ProviderAttempt"
        | "provider_attempt"
        | "ContextEnvelope"
        | "context_envelope"
        | "ContextWindow"
        | "context_window"
        | "TokenUsage"
        | "token_usage"
        | "RunModelTelemetry"
        | "run_model_telemetry" => require_execution(),
        _ => Ok(()),
    }
}

fn gateway_sse_frame_commit_cursor(frame: &str) -> Option<u64> {
    frame
        .lines()
        .find_map(|line| line.strip_prefix("id:"))
        .map(str::trim)
        .and_then(|value| value.parse::<u64>().ok())
}

fn validate_execution_projection_identity(
    requested_execution_id: &str,
    projected_execution_id: &str,
) -> Result<(), GatewayApiError> {
    if requested_execution_id == projected_execution_id {
        return Ok(());
    }
    Err(GatewayApiError::Contract(format!(
        "requested execution `{requested_execution_id}` but Gateway projected foreign execution `{projected_execution_id}`"
    )))
}

fn validate_session_json_identity(
    requested_session_id: &str,
    value: &serde_json::Value,
    contract: &str,
) -> Result<(), GatewayApiError> {
    validate_session_json_identity_at(requested_session_id, value, contract, &["/session_id"])
}

fn validate_session_json_identity_at(
    requested_session_id: &str,
    value: &serde_json::Value,
    contract: &str,
    pointers: &[&str],
) -> Result<(), GatewayApiError> {
    let projected_session_id = pointers
        .iter()
        .find_map(|pointer| {
            value
                .pointer(pointer)
                .and_then(serde_json::Value::as_str)
                .filter(|session_id| !session_id.trim().is_empty())
        })
        .ok_or_else(|| {
            GatewayApiError::Contract(format!(
                "{contract} is missing a non-empty session identity at {}",
                pointers.join(" or ")
            ))
        })?;
    if projected_session_id != requested_session_id {
        return Err(GatewayApiError::Contract(format!(
            "requested session `{requested_session_id}` but Gateway returned {contract} for `{projected_session_id}`"
        )));
    }
    Ok(())
}

fn validate_session_messages_identity(
    requested_session_id: &str,
    page: &SessionMessagesPage,
) -> Result<(), GatewayApiError> {
    if page.session_id != requested_session_id {
        return Err(GatewayApiError::Contract(format!(
            "requested session `{requested_session_id}` but Gateway returned history for `{}`",
            page.session_id
        )));
    }
    if let Some(foreign) = page
        .messages
        .iter()
        .find(|message| message.session_id != requested_session_id)
    {
        return Err(GatewayApiError::Contract(format!(
            "history page for `{requested_session_id}` contains foreign message `{}` from session `{}`",
            foreign.id, foreign.session_id
        )));
    }
    Ok(())
}

#[cfg(test)]
fn gateway_sse_frame_resync_reason(frame: &str) -> Option<String> {
    let data = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    let value = serde_json::from_str::<serde_json::Value>(&data).ok()?;
    match value.get("type").and_then(serde_json::Value::as_str) {
        Some("session_stream_resync") => Some(
            value
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("transport resync")
                .to_string(),
        ),
        Some("RuntimeStreamLagged") => Some(format!(
            "runtime relay lag ({} events skipped)",
            value
                .get("skipped")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default()
        )),
        _ => None,
    }
}

#[cfg(test)]
fn validate_session_authorization_revoke_identity(
    frame: &str,
    subscribed_session_id: &str,
) -> Result<(), GatewayApiError> {
    let data = gateway_sse_frame_data(frame).ok_or_else(|| {
        GatewayApiError::Contract("session authorization revoke frame has no JSON data".to_string())
    })?;
    let value = serde_json::from_str::<serde_json::Value>(&data).map_err(|error| {
        GatewayApiError::Contract(format!(
            "invalid session authorization revoke JSON: {error}"
        ))
    })?;
    validate_gateway_session_event_contract(
        &value,
        "SessionAuthorizationRevoked",
        subscribed_session_id,
        gateway_sse_frame_commit_cursor(frame),
    )
    .map_err(GatewayApiError::Contract)
}

fn gateway_sse_frame_event_name(frame: &str) -> Option<&str> {
    frame
        .lines()
        .find_map(|line| line.strip_prefix("event:"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn gateway_sse_frame_id(frame: &str) -> Option<&str> {
    frame.lines().find_map(|line| {
        line.strip_prefix("id:")
            .map(str::trim)
            .filter(|value| !value.is_empty())
    })
}

fn gateway_sse_frame_data(frame: &str) -> Option<String> {
    let data = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    (!data.is_empty()).then_some(data)
}

#[cfg(test)]
fn gateway_sse_frame_projection_delta(
    frame: &str,
) -> Option<harness_contract::projection::ProjectionDelta> {
    (gateway_sse_frame_event_name(frame) == Some("projection_delta"))
        .then(|| gateway_sse_frame_data(frame))
        .flatten()
        .and_then(|data| serde_json::from_str(&data).ok())
}

fn gateway_sse_frame_projection_authorization_revoked(frame: &str) -> bool {
    gateway_sse_frame_event_name(frame) == Some("projection_authorization_revoked")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn session_text_delta_preserves_runtime_causal_and_projection_identities() {
        let frame = concat!(
            "id: 7\n",
            "data: {",
            "\"type\":\"TextDelta\",",
            "\"session_id\":\"session-causal\",",
            "\"execution_id\":\"execution-causal\",",
            "\"turn_id\":\"turn-causal\",",
            "\"model_step_id\":\"step-causal\",",
            "\"item_id\":\"text-causal\",",
            "\"segment_id\":\"text-causal:text:0\",",
            "\"part_id\":\"execution-causal:assistant-output\",",
            "\"causal_sequence\":3,",
            "\"delta_sequence\":1,",
            "\"text\":\"ok\",",
            "\"start_bytes\":0,",
            "\"end_bytes\":2,",
            "\"stream_revision\":2",
            "}\n\n"
        );
        let event = strict_gateway_sse_frame_to_cowd_event_for_session(frame, "session-causal")
            .expect("valid causal frame")
            .expect("typed event");
        let CowdEvent::GatewaySession {
            event:
                GatewaySessionEvent::TextDelta {
                    correlation, text, ..
                },
        } = event
        else {
            panic!("expected causal text delta");
        };
        assert_eq!(text, "ok");
        assert_eq!(
            correlation.part_id.as_deref(),
            Some("execution-causal:assistant-output")
        );
        assert_eq!(correlation.item_id.as_deref(), Some("text-causal"));
        assert_eq!(
            correlation.segment_id.as_deref(),
            Some("text-causal:text:0")
        );
    }

    #[test]
    fn tui_consumes_the_canonical_live_envelope_fixture() {
        let envelope: harness_contract::live::LiveEnvelope =
            serde_json::from_str(harness_contract::live::LIVE_ENVELOPE_CANONICAL_FIXTURE_JSON)
                .expect("TUI must consume the canonical live fixture");
        assert_eq!(envelope.subscription_id, "subscription-contract");
        assert_eq!(envelope.subscription_revision, 7);
        assert_eq!(envelope.source_cursor, Some(42));
        assert_eq!(envelope.event, "TerminalCommitted");
        assert_eq!(
            harness_contract::live::live_envelope_schema_hash(),
            "53ccc1bb8fb6896f1e648035dad6985aba8754b2e5d88e47b7687ddc492a346c"
        );
    }

    #[test]
    fn live_source_selector_recomputes_detail_without_regressing_cursor() {
        let (summary_tx, _) = mpsc::channel(4);
        let (full_tx, _) = mpsc::channel(4);
        let mut source = LiveSourceState {
            selector: harness_contract::live::LiveSourceSelector {
                kind: harness_contract::live::LiveSourceKind::Execution,
                id: "execution-1".to_string(),
                cursor: 11,
                revision: 1,
                detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
            },
            subscribers: BTreeMap::from([
                (
                    "summary".to_string(),
                    LiveSubscriber {
                        selector: harness_contract::live::LiveSourceSelector {
                            kind: harness_contract::live::LiveSourceKind::Execution,
                            id: "execution-1".to_string(),
                            cursor: 5,
                            revision: 1,
                            detail_scope:
                                harness_contract::projection::ProjectionDetailScope::Summary,
                        },
                        tx: summary_tx,
                    },
                ),
                (
                    "full".to_string(),
                    LiveSubscriber {
                        selector: harness_contract::live::LiveSourceSelector {
                            kind: harness_contract::live::LiveSourceKind::Execution,
                            id: "execution-1".to_string(),
                            cursor: 7,
                            revision: 1,
                            detail_scope: harness_contract::projection::ProjectionDetailScope::Full,
                        },
                        tx: full_tx,
                    },
                ),
            ]),
            pending_previews: BTreeMap::new(),
        };

        refresh_tui_live_source_selector(&mut source);
        assert_eq!(
            source.selector.detail_scope,
            harness_contract::projection::ProjectionDetailScope::Full
        );
        assert_eq!(source.selector.cursor, 11);

        source.subscribers.remove("full");
        refresh_tui_live_source_selector(&mut source);
        assert_eq!(
            source.selector.detail_scope,
            harness_contract::projection::ProjectionDetailScope::Summary
        );
        assert_eq!(source.selector.cursor, 11);
    }

    #[tokio::test]
    async fn live_transport_interruption_is_visible_to_every_source() {
        let (session_tx, mut session_rx) = mpsc::channel(4);
        let (mission_tx, mut mission_rx) = mpsc::channel(4);
        let session_selector = harness_contract::live::LiveSourceSelector {
            kind: harness_contract::live::LiveSourceKind::Session,
            id: "session-1".to_string(),
            cursor: 13,
            revision: 0,
            detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
        };
        let mission_selector = harness_contract::live::LiveSourceSelector {
            kind: harness_contract::live::LiveSourceKind::Mission,
            id: "mission-1".to_string(),
            cursor: 0,
            revision: 0,
            detail_scope: harness_contract::projection::ProjectionDetailScope::Full,
        };
        let mut sources = BTreeMap::from([
            (
                session_selector.key(),
                LiveSourceState {
                    selector: session_selector.clone(),
                    subscribers: BTreeMap::from([(
                        "session-view".to_string(),
                        LiveSubscriber {
                            selector: session_selector,
                            tx: session_tx,
                        },
                    )]),
                    pending_previews: BTreeMap::new(),
                },
            ),
            (
                mission_selector.key(),
                LiveSourceState {
                    selector: mission_selector.clone(),
                    subscribers: BTreeMap::from([(
                        "mission-view".to_string(),
                        LiveSubscriber {
                            selector: mission_selector,
                            tx: mission_tx,
                        },
                    )]),
                    pending_previews: BTreeMap::new(),
                },
            ),
        ]);

        deliver_tui_live_resync(&mut sources, &None, "gateway unavailable").await;

        let session = session_rx.recv().await.expect("session resync");
        let mission = mission_rx.recv().await.expect("mission resync");
        assert_eq!(
            session.source_health,
            harness_contract::live::SourceHealth::ResyncRequired
        );
        assert_eq!(
            mission.source_health,
            harness_contract::live::SourceHealth::ResyncRequired
        );
        assert_eq!(session.source_cursor, Some(13));
        assert_eq!(session.payload["reason"], "gateway unavailable");
        assert_eq!(mission.payload["origin"], "tui_live_transport");
    }

    #[tokio::test]
    async fn snapshot_reconstructable_events_use_reliable_bounded_delivery() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(harness_contract::live::canonical_live_envelope_fixture())
            .await
            .expect("prefill subscriber queue");
        let selector = harness_contract::live::LiveSourceSelector {
            kind: harness_contract::live::LiveSourceKind::Session,
            id: "session-contract".to_string(),
            cursor: 42,
            revision: 0,
            detail_scope: harness_contract::projection::ProjectionDetailScope::Full,
        };
        let mut sources = BTreeMap::from([(
            selector.key(),
            LiveSourceState {
                selector: selector.clone(),
                subscribers: BTreeMap::from([(
                    "execution-view".to_string(),
                    LiveSubscriber { selector, tx },
                )]),
                pending_previews: BTreeMap::new(),
            },
        )]);
        let mut envelope = harness_contract::live::canonical_live_envelope_fixture();
        envelope.delivery_class = harness_contract::live::DeliveryClass::SnapshotReconstructable;
        envelope.event = "ExecutionPhase".to_string();

        let mut delivery = Box::pin(deliver_tui_live_envelope(&mut sources, envelope));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut delivery)
                .await
                .is_err(),
            "reconstructable event must wait instead of replacing queued causal data"
        );
        assert_eq!(
            rx.recv().await.expect("prefilled event").event,
            "TerminalCommitted"
        );
        tokio::time::timeout(Duration::from_secs(1), delivery)
            .await
            .expect("reliable delivery resumes when bounded capacity is available");
        assert_eq!(
            rx.recv().await.expect("reconstructable event").event,
            "ExecutionPhase"
        );
    }

    #[tokio::test]
    async fn session_text_delta_backpressures_instead_of_restarting_a_saturated_source() {
        let (tx, mut rx) = crate::cowd_event_channel();
        for index in 0..256 {
            tx.send(CowdEvent::ReasoningSummaryDelta {
                summary: format!("queued-{index}"),
            })
            .expect("fill primary event queue");
        }
        let event = CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TextDelta {
                correlation: GatewayEventCorrelation {
                    session_id: "session-1".to_string(),
                    execution_id: Some("execution-1".to_string()),
                    turn_id: Some("turn-1".to_string()),
                    part_id: Some("item-text-1:text:0".to_string()),
                    ..Default::default()
                },
                text: "visible before terminal".to_string(),
                start_bytes: 0,
                end_bytes: 23,
                stream_revision: 23,
            },
        };

        let mut delivery = Box::pin(deliver_session_stream_event(&tx, "session-1", event, 1));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut delivery)
                .await
                .is_err(),
            "a saturated render queue must apply backpressure instead of failing the live source"
        );
        assert!(matches!(
            rx.try_recv().expect("free one queue slot"),
            CowdEvent::ReasoningSummaryDelta { .. }
        ));
        tokio::time::timeout(Duration::from_secs(1), delivery)
            .await
            .expect("delta delivery resumes when one bounded slot is available")
            .expect("healthy live source remains connected");

        let delivered = std::iter::from_fn(|| rx.try_recv().ok()).any(|event| {
            matches!(
                event,
                CowdEvent::SessionScoped {
                    session_id,
                    authority_generation: 1,
                    event,
                } if session_id == "session-1"
                    && matches!(
                        event.as_ref(),
                        CowdEvent::GatewaySession {
                            event: GatewaySessionEvent::TextDelta { text, .. }
                        } if text == "visible before terminal"
                    )
            )
        });
        assert!(
            delivered,
            "accepted preview must reach the TUI reducer exactly once"
        );
    }

    #[test]
    fn normalize_base_url_trims_trailing_slashes() {
        assert_eq!(
            normalize_base_url(" http://127.0.0.1:8642/// ".to_string()).unwrap(),
            "http://127.0.0.1:8642"
        );
        assert!(normalize_base_url("127.0.0.1:8642".to_string()).is_err());
    }

    #[test]
    fn url_encode_encodes_session_ids() {
        assert_eq!(url_encode("session a/b"), "session%20a%2Fb");
    }

    #[test]
    fn generic_app_transport_accepts_only_local_paths_and_non_reserved_metadata() {
        assert_eq!(app_method("post").expect("method"), reqwest::Method::POST);
        assert!(validate_app_path("/api/apps/fixture/read").is_ok());
        assert!(validate_app_path("https://example.invalid/api/apps/fixture/read").is_err());
        assert!(validate_app_path("/api/apps/fixture/../admin").is_err());
        assert!(validate_app_path("/api/apps/fixture/%2e%2e/admin").is_err());
        assert!(app_headers(&BTreeMap::from([(
            "x-fixture-cursor".to_string(),
            "42".to_string(),
        )]))
        .is_ok());
        assert!(app_headers(&BTreeMap::from([(
            "authorization".to_string(),
            "Bearer leaked".to_string(),
        )]))
        .is_err());
    }

    #[test]
    fn tui_gateway_request_delegates_capability_projection_to_gateway_catalog() {
        let request = authorize_tui_request(
            reqwest::Client::new().get("http://127.0.0.1:8642/healthz"),
            Some("  test-token  "),
            "tui:test-observer",
        )
        .build()
        .expect("decorated request should build");
        let headers = request.headers();

        assert_eq!(
            headers
                .get("x-cowd-surface-id")
                .and_then(|value| value.to_str().ok()),
            Some("tui")
        );
        assert_eq!(
            headers
                .get("x-cowd-observer-id")
                .and_then(|value| value.to_str().ok()),
            Some("tui:test-observer")
        );
        assert_eq!(
            headers
                .get(reqwest::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer test-token")
        );
        assert!(headers.get("x-cowd-requested-capabilities").is_none());
    }

    #[test]
    fn gateway_sse_json_maps_core_cowd_events() {
        assert!(matches!(
            gateway_sse_json_to_cowd_event_for_session(
                &serde_json::json!({
                    "type": "UserMessageCommitted",
                    "message_id": "tui:message-1",
                    "sequence": 7,
                    "execution_id": "execution-1",
                    "turn_id": "turn-1",
                    "content": "hello",
                    "created_at_ms": 42
                }),
                Some("session-1")
            ),
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::UserMessageCommitted {
                    correlation: GatewayEventCorrelation {
                        session_id,
                        message_id: Some(message_id),
                        execution_id: Some(execution_id),
                        turn_id: Some(turn_id),
                        ..
                    },
                    sequence: 7,
                    ..
                }
            }) if session_id == "session-1"
                && message_id == "tui:message-1"
                && execution_id == "execution-1"
                && turn_id == "turn-1"
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "TextDelta",
                "text": "hello",
                "start_bytes": 0,
                "end_bytes": 5,
                "stream_revision": 5
            })),
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::TextDelta { .. }
            })
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "ReasoningSummaryDelta",
                "summary": "checking"
            })),
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ReasoningSummaryDelta { .. }
            })
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "ToolStart",
                "id": "tool-1",
                "name": "read"
            })),
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ToolStart { .. }
            })
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "TerminalCommitted",
                "terminal_id": "terminal-1",
                "response": "done"
            })),
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::TerminalCommitted { .. }
            })
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "TurnComplete",
                "assistant_text": "draft"
            })),
            None
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "ExecutionPhase",
                "status": "CallingModel",
                "detail": "requesting model"
            })),
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ExecutionPhase { .. }
            })
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "SessionInputProjection",
                "projection": {
                    "session_id": "session-1",
                    "pending_count": 0,
                    "inputs": []
                }
            })),
            Some(CowdEvent::SessionInputProjection { projection })
                if projection["pending_count"] == 0
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "TurnInboxUpdated",
                "inbox": {
                    "session_id": "session-1",
                    "pending_count": 0,
                    "items": []
                }
            })),
            Some(CowdEvent::SessionInputProjection { projection })
                if projection["inputs"].as_array().is_some_and(Vec::is_empty)
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "ContextEnvelope",
                "envelope": {
                    "id": "ctx-v31",
                    "selected": []
                }
            })),
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ContextEnvelope { .. }
            })
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "TokenUsage",
                "input": 1,
                "output": 2,
                "cache_create": 3,
                "cache_read": 4
            })),
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::TokenUsage { .. }
            })
        ));
    }

    #[test]
    fn gateway_sse_frame_reads_durable_commit_cursor_from_event_id() {
        assert_eq!(
            gateway_sse_frame_commit_cursor("id: 73\ndata: {\"type\":\"TerminalCommitted\"}"),
            Some(73)
        );
        assert_eq!(
            gateway_sse_frame_commit_cursor("data: {\"type\":\"TextDelta\"}"),
            None
        );
    }

    #[test]
    fn gateway_sse_frame_parses_canonical_projection_delta_only_from_named_event() {
        let delta = serde_json::json!({
            "schema_version": 2,
            "reducer_version": 1,
            "execution_id": "graph-1",
            "from_revision": 1,
            "target_revision": 1,
            "base_cursor": 4,
            "target_cursor": 5,
            "detail_scope": "summary",
            "authorization_revision": 1,
            "redaction_revision": "sha256:test",
            "source_health": "fresh",
            "operations": [
                {
                    "op": "advance_cursor",
                    "cursor": 5
                }
            ],
            "resync_reason": null
        });
        let frame = format!("id: 5\nevent: projection_delta\ndata: {delta}");
        let parsed = gateway_sse_frame_projection_delta(&frame).expect("projection delta");
        assert_eq!(parsed.execution_id, "graph-1");
        assert_eq!(parsed.target_cursor, 5);
        assert!(gateway_sse_frame_projection_delta(&format!("data: {delta}")).is_none());
    }

    #[tokio::test]
    async fn e10_projection_revocation_and_unknown_event_fail_closed_before_any_delta() {
        assert!(gateway_sse_frame_projection_authorization_revoked(
            "event: projection_authorization_revoked\ndata: {\"reason\":\"credential epoch changed\"}"
        ));
        assert!(!gateway_sse_frame_projection_authorization_revoked(
            "event: projection_delta\ndata: {}"
        ));

        let client = GatewayApiClient::new("http://127.0.0.1:1".to_string(), None).expect("client");
        let (tx, _rx) = crate::cowd_event_channel();
        let revoked = client
            .apply_execution_projection_sse_frame(
                "event: projection_authorization_revoked\ndata: {\"reason\":\"credential epoch changed\"}",
                "execution-e10",
                true,
                7,
                0,
                &tx,
            )
            .await
            .expect_err("revocation must terminate the projection stream");
        assert!(matches!(
            revoked,
            GatewayApiError::Status(reqwest::StatusCode::FORBIDDEN, message)
                if message.contains("revoked")
        ));

        let unknown = client
            .apply_execution_projection_sse_frame(
                "event: future_unregistered_projection\ndata: {}",
                "execution-e10",
                true,
                7,
                0,
                &tx,
            )
            .await
            .expect_err("unknown projection events must not mutate local state");
        assert!(matches!(
            unknown,
            GatewayApiError::Contract(message)
                if message.contains("unknown event `future_unregistered_projection`")
        ));
        assert!(matches!(
            validate_execution_projection_identity("execution-e10", "foreign-execution"),
            Err(GatewayApiError::Contract(message))
                if message.contains("foreign execution")
        ));
    }

    #[test]
    fn session_sse_rejects_explicit_foreign_identity_for_all_ui_event_classes() {
        for event_type in ["Warning", "RuntimePolicyDecision", "ExecutionGraphSummary"] {
            let frame = format!(
                "event: message\ndata: {{\"type\":\"{event_type}\",\"session_id\":\"foreign-session\",\"message\":\"foreign\"}}\n\n"
            );
            let error =
                strict_gateway_sse_frame_to_cowd_event_for_session(&frame, "session-current")
                    .expect_err("an explicit foreign session must fail closed before parsing");
            assert!(
                error.contains("does not match subscribed session"),
                "{event_type}: {error}"
            );
        }
        assert!(matches!(
            validate_session_authorization_revoke_identity(
                "event: message\ndata: {\"type\":\"SessionAuthorizationRevoked\",\"session_id\":\"foreign-session\",\"reason\":\"foreign revoke\"}\n\n",
                "session-current",
            ),
            Err(GatewayApiError::Contract(message))
                if message.contains("does not match subscribed session")
        ));
    }

    #[tokio::test]
    async fn session_http_contracts_reject_foreign_projection_history_input_and_index() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let responses = [
                (
                    "/api/sessions/session-current/projection",
                    serde_json::json!({"session_id":"foreign-session","turns":[]}),
                ),
                (
                    "/api/sessions/session-current/input-projection",
                    serde_json::json!({
                        "session_id":"foreign-session",
                        "total":0,
                        "pending_count":0,
                        "queued_next_count":0,
                        "consumed_count":0,
                        "inputs":[],
                        "updated_at":"2026-07-24T00:00:00Z"
                    }),
                ),
                (
                    "/api/sessions/session-current/messages?offset=0&limit=1",
                    serde_json::json!({
                        "session_id":"foreign-session",
                        "messages":[],
                        "total":0,
                        "offset":0,
                        "next_seq":0,
                        "limit":1,
                        "has_more":false
                    }),
                ),
                (
                    "/api/sessions/session-current/execution",
                    serde_json::json!({
                        "session_id":"foreign-session",
                        "active_execution_ids":[]
                    }),
                ),
            ];
            for (path, body) in responses {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut request = vec![0; 4096];
                let size = socket.read(&mut request).await.expect("read");
                let request = String::from_utf8_lossy(&request[..size]);
                assert!(request.starts_with(&format!("GET {path} HTTP/1.1")));
                let body = body.to_string();
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write");
            }
        });
        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");

        assert!(matches!(
            client.session_projection("session-current").await,
            Err(GatewayApiError::Contract(message)) if message.contains("foreign-session")
        ));
        assert!(matches!(
            client.session_input_projection("session-current").await,
            Err(GatewayApiError::Contract(message)) if message.contains("foreign-session")
        ));
        assert!(matches!(
            client
                .session_messages_offset("session-current", 0, 1)
                .await,
            Err(GatewayApiError::Contract(message)) if message.contains("foreign-session")
        ));
        assert!(matches!(
            client.session_execution_index("session-current").await,
            Err(GatewayApiError::Contract(message)) if message.contains("foreign-session")
        ));
        server.await.expect("server joins");
    }

    #[tokio::test]
    async fn ensure_session_http_receipt_rejects_a_foreign_session_identity() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut request = vec![0; 4096];
            let size = socket.read(&mut request).await.expect("read");
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(
                request.starts_with("POST /api/sessions/session-current/ensure HTTP/1.1"),
                "{request}"
            );
            let body = serde_json::json!({"ok":true,"session_id":"foreign-session"}).to_string();
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write");
        });
        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");

        assert!(matches!(
            client.ensure_session("session-current", "model").await,
            Err(GatewayApiError::Contract(message)) if message.contains("foreign-session")
        ));
        server.await.expect("server joins");
    }

    #[test]
    fn every_session_operation_receipt_requires_the_requested_identity() {
        assert!(matches!(
            require_gateway_session_operation_ok(
                serde_json::json!({"ok":true,"session_id":"foreign-session"}),
                "ensure session",
                "session-current",
            ),
            Err(GatewayApiError::Contract(message))
                if message.contains("foreign-session")
        ));
        assert!(matches!(
            require_gateway_session_operation_ok(
                serde_json::json!({"ok":true}),
                "attach session",
                "session-current",
            ),
            Err(GatewayApiError::Contract(message))
                if message.contains("missing")
        ));
        assert!(validate_session_json_identity_at(
            "session-current",
            &serde_json::json!({
                "resource": {"session_id":"session-current"}
            }),
            "resource upload receipt",
            &["/resource/session_id"],
        )
        .is_ok());
        assert!(matches!(
            validate_session_json_identity_at(
                "session-current",
                &serde_json::json!({
                    "resource": {"session_id":"foreign-session"}
                }),
                "resource upload receipt",
                &["/resource/session_id"],
            ),
            Err(GatewayApiError::Contract(message))
                if message.contains("foreign-session")
        ));
    }

    #[test]
    fn gateway_sse_frame_maps_data_json() {
        let event = gateway_sse_frame_to_cowd_event(concat!(
            "event: message\n",
            "data: {",
            "\"type\":\"TextDelta\",",
            "\"session_id\":\"session-1\",",
            "\"execution_id\":\"execution-1\",",
            "\"turn_id\":\"turn-1\",",
            "\"model_step_id\":\"step-1\",",
            "\"item_id\":\"text-1\",",
            "\"segment_id\":\"text-1:text:0\",",
            "\"part_id\":\"execution-1:assistant-output\",",
            "\"causal_sequence\":1,",
            "\"delta_sequence\":1,",
            "\"text\":\"hi\",",
            "\"start_bytes\":0,",
            "\"end_bytes\":2,",
            "\"stream_revision\":2",
            "}\n\n"
        ))
        .expect("typed causal SSE event");
        assert!(matches!(
            event,
            CowdEvent::GatewaySession {
                event: GatewaySessionEvent::TextDelta {
                    correlation: GatewayEventCorrelation {
                        model_step_id: Some(model_step_id),
                        item_id: Some(item_id),
                        segment_id: Some(segment_id),
                        ..
                    },
                    ..
                }
            } if model_step_id == "step-1"
                && item_id == "text-1"
                && segment_id == "text-1:text:0"
        ));
        assert!(gateway_sse_frame_to_cowd_event("data: [DONE]\n\n").is_none());
        assert_eq!(
            gateway_sse_frame_resync_reason(
                "data: {\"type\":\"session_stream_resync\",\"reason\":\"transport_lag\"}\n\n"
            )
            .as_deref(),
            Some("transport_lag")
        );
        assert_eq!(
            gateway_sse_frame_resync_reason(
                "data: {\"type\":\"RuntimeStreamLagged\",\"skipped\":7}\n\n"
            )
            .as_deref(),
            Some("runtime relay lag (7 events skipped)")
        );
    }

    #[tokio::test]
    async fn typed_evolution_and_managed_agent_controls_use_gateway_owned_routes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let checks = [
                (
                    "GET /api/evolution/evaluation-policy HTTP/1.1",
                    Vec::<&str>::new(),
                ),
                (
                    "GET /api/evolution/evaluation-policy/reviews HTTP/1.1",
                    Vec::<&str>::new(),
                ),
                (
                    "POST /api/evolution/reviews/release-1/decision HTTP/1.1",
                    vec![
                        "\"decision\":\"approve\"",
                        "\"reason\":\"operator checked\"",
                    ],
                ),
                (
                    "POST /api/evolution/evaluation-policy/reviews/policy-1/decision HTTP/1.1",
                    vec!["\"decision\":\"reject\"", "\"reason\":\"operator checked\""],
                ),
                (
                    "GET /api/runtime/managed-agents HTTP/1.1",
                    Vec::<&str>::new(),
                ),
                (
                    "POST /api/runtime/managed-agents/dispatch HTTP/1.1",
                    vec!["\"dispatcher_id\":\"tui-operator\"", "\"limit\":16"],
                ),
                (
                    "POST /api/runtime/managed-agents/agent-1/health/reset HTTP/1.1",
                    Vec::<&str>::new(),
                ),
            ];
            for (expected_start, expected_fragments) in checks {
                let (mut socket, _) = listener.accept().await.expect("accept request");
                let mut buf = vec![0; 4096];
                let n = socket.read(&mut buf).await.expect("read request");
                let request = String::from_utf8_lossy(&buf[..n]);
                assert!(request.starts_with(expected_start), "request was {request}");
                for fragment in expected_fragments {
                    assert!(request.contains(fragment), "request was {request}");
                }
                assert!(
                    !request.contains("actor_principal"),
                    "TUI must not supply an approval actor: {request}"
                );
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                    )
                    .await
                    .expect("write response");
            }
        });

        let client =
            GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
                .expect("client");
        assert_eq!(
            client.evolution_evaluation_policy().await.expect("policy")["ok"],
            true
        );
        assert_eq!(
            client
                .evolution_evaluation_policy_reviews()
                .await
                .expect("policy reviews")["ok"],
            true
        );
        assert_eq!(
            client
                .evolution_review_decision("release-1", "approve", "operator checked")
                .await
                .expect("release decision")["ok"],
            true
        );
        assert_eq!(
            client
                .evolution_evaluation_policy_review_decision(
                    "policy-1",
                    "reject",
                    "operator checked",
                )
                .await
                .expect("policy decision")["ok"],
            true
        );
        assert_eq!(client.managed_agents().await.expect("agents")["ok"], true);
        assert_eq!(
            client
                .dispatch_managed_agents("tui-operator", 16)
                .await
                .expect("dispatch")["ok"],
            true
        );
        assert_eq!(
            client
                .reset_managed_agent_health("agent-1")
                .await
                .expect("health reset")["ok"],
            true
        );
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn session_history_index_is_typed_bounded_and_session_scoped() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with(
                "GET /api/sessions/session-1/history-index?metadata_limit=128&card_limit=64 HTTP/1.1"
            ));
            assert!(req.contains("authorization: Bearer test-token"));
            let body = serde_json::json!({
                "schema_version": 1,
                "session_id": "session-1",
                "projection_generation": 9,
                "durable_cursor": 42,
                "event_cursor": 41,
                "history_revision": 7,
                "total_messages": 100000,
                "total_bytes": 8000000,
                "latest_checkpoint_sequence": 90000,
                "latest_checkpoint_event_id": "checkpoint-1",
                "index_generation": 4,
                "indexed_through_sequence": 99999,
                "index_card_count": 250,
                "index_complete": true,
                "recovery_state": "ready",
                "recent_metadata": [],
                "cards": []
            })
            .to_string();
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write");
        });

        let client =
            GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
                .expect("client");
        let projection = client
            .session_history_index("session-1")
            .await
            .expect("history index");
        assert_eq!(projection.session_id, "session-1");
        assert_eq!(projection.total_messages, 100_000);
        assert_eq!(projection.projection_generation, 9);
        assert!(projection.recent_metadata.is_empty());
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn runtime_control_plane_gets_json_with_auth() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("GET /api/runtime/control-plane HTTP/1.1"));
            assert!(req.contains("authorization: Bearer test-token"));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write");
        });

        let client =
            GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
                .expect("client");
        let json = client.runtime_control_plane().await.expect("json");
        assert_eq!(json["ok"], true);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn session_stream_holds_live_delta_until_revision_barrier_then_hydrates_history() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut subscription_socket, _) =
                listener.accept().await.expect("accept subscription");
            let mut request = vec![0; 4096];
            let size = subscription_socket
                .read(&mut request)
                .await
                .expect("read subscription request");
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(
                request.starts_with("POST /api/runtime/live-subscriptions HTTP/1.1"),
                "{request}"
            );
            let subscription = serde_json::json!({
                "schema_version": 1,
                "id": "live-test",
                "surface_instance": "tui:test",
                "revision": 1,
                "selector": {"sources": [{
                    "kind": "session",
                    "id": "session-1",
                    "cursor": 0,
                    "detail_scope": "summary"
                }]},
                "selector_hash": "selector-1",
                "expires_at_ms": u64::MAX,
                "stream_url": "/api/runtime/live/live-test"
            })
            .to_string();
            subscription_socket
                .write_all(
                    format!(
                        "HTTP/1.1 201 Created\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{subscription}",
                        subscription.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write subscription response");
            drop(subscription_socket);

            let (mut stream_socket, _) = listener.accept().await.expect("accept stream");
            let mut request = vec![0; 4096];
            let size = stream_socket
                .read(&mut request)
                .await
                .expect("read stream request");
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(
                request.starts_with("GET /api/runtime/live/live-test HTTP/1.1"),
                "{request}"
            );
            let ready = serde_json::json!({
                "schema_version": 1,
                "subscription_id": "live-test",
                "subscription_revision": 1,
                "source_kind": "subscription",
                "source_id": "live-test",
                "detail_scope": "summary",
                "delivery_class": "snapshot_reconstructable",
                "source_health": "baseline",
                "event": "subscription.ready",
                "payload": {"revision": 1}
            });
            let terminal = serde_json::json!({
                "schema_version": 1,
                "subscription_id": "live-test",
                "subscription_revision": 1,
                "source_kind": "session",
                "source_id": "session-1",
                "detail_scope": "summary",
                "source_cursor": 9,
                "delivery_class": "durable",
                "source_health": "live",
                "event": "TerminalCommitted",
                "payload": {
                    "type": "TerminalCommitted",
                    "session_id": "session-1",
                    "execution_id": "execution-1",
                    "turn_id": "turn-1",
                    "part_id": "item-text-1:text:0",
                    "message_id": "assistant-2",
                    "terminal_id": "terminal-1",
                    "response": "live answer"
                }
            });
            let resync = serde_json::json!({
                "schema_version": 1,
                "subscription_id": "live-test",
                "subscription_revision": 1,
                "source_kind": "session",
                "source_id": "session-1",
                "detail_scope": "summary",
                "source_cursor": 9,
                "delivery_class": "snapshot_reconstructable",
                "source_health": "resync_required",
                "event": "source.resync_required",
                "payload": {"reason": "fixture complete"}
            });
            stream_socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: keep-alive\r\n\r\nevent: live\r\ndata: {terminal}\r\n\r\nevent: live\r\ndata: {ready}\r\n\r\nevent: live\r\ndata: {resync}\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .expect("write stream response");

            let (mut probe_socket, _) = listener.accept().await.expect("accept history index");
            let mut request = vec![0; 4096];
            let size = probe_socket
                .read(&mut request)
                .await
                .expect("read history index");
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(
                request.starts_with(
                    "GET /api/sessions/session-1/history-index?metadata_limit=128&card_limit=64 HTTP/1.1"
                ),
                "{request}"
            );
            let history_index = serde_json::json!({
                "schema_version": 1,
                "session_id": "session-1",
                "projection_generation": 1,
                "durable_cursor": 1,
                "event_cursor": 1,
                "history_revision": 1,
                "total_messages": 1,
                "total_bytes": 64,
                "latest_checkpoint_sequence": null,
                "latest_checkpoint_event_id": null,
                "index_generation": 1,
                "indexed_through_sequence": 0,
                "index_card_count": 1,
                "index_complete": true,
                "recovery_state": "ready",
                "recent_metadata": [],
                "cards": []
            })
            .to_string();
            probe_socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{history_index}",
                        history_index.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write history index");
            let history = serde_json::json!({
                "session_id": "session-1",
                "messages": [{
                    "id": "user-1",
                    "session_id": "session-1",
                    "sequence": 0,
                    "role": "user",
                    "blocks": [{"type": "text", "text": "historical question"}],
                    "created_at_ms": 1
                }],
                "total": 1,
                "offset": 0,
                "from_seq": 0,
                "next_seq": 1,
                "limit": 500,
                "has_more": false
            })
            .to_string();
            let (mut history_socket, _) = listener.accept().await.expect("accept history page");
            let mut request = vec![0; 4096];
            let size = history_socket
                .read(&mut request)
                .await
                .expect("read history page");
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(
                request.starts_with(
                    "GET /api/sessions/session-1/messages?offset=0&limit=500 HTTP/1.1"
                ),
                "{request}"
            );
            history_socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{history}",
                        history.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write history page");
            drop(stream_socket);
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let (tx, mut rx) = crate::cowd_event_channel();
        let progress = client
            .consume_session_live_source(
                "session-1",
                tx.clone(),
                None,
                Arc::new(AtomicUsize::new(0)),
                1,
            )
            .await
            .expect("subscribe");
        assert_eq!(progress.commit_cursor, Some(9));
        assert_eq!(progress.next_message_sequence, 1);
        drop(tx);

        let events = std::iter::from_fn(|| rx.try_recv().ok()).collect::<Vec<_>>();
        assert!(events.iter().any(|event| matches!(
            event,
            CowdEvent::SessionScoped { session_id, event, .. }
                if session_id == "session-1"
                    && matches!(event.as_ref(), CowdEvent::SessionHistoryPage {
                        page: SessionMessagesPage {
                            session_id,
                            has_more: false,
                            ..
                        }
                    } if session_id == "session-1")
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            CowdEvent::SessionScoped { session_id, event, .. }
                if session_id == "session-1"
                    && matches!(event.as_ref(), CowdEvent::SessionStreamConnection {
                        session_id,
                        state: SessionStreamConnectionState::Connected
                    } if session_id == "session-1")
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            CowdEvent::SessionScoped {
                session_id, event, ..
            }
                if session_id == "session-1"
                    && matches!(event.as_ref(), CowdEvent::GatewaySession {
                        event: GatewaySessionEvent::TerminalCommitted {
                            correlation: GatewayEventCorrelation {
                                message_id: Some(message_id),
                                terminal_id: Some(terminal_id),
                                ..
                            },
                            assistant_text,
                            ..
                        }
                    } if message_id == "assistant-2"
                        && terminal_id == "terminal-1"
                        && assistant_text == "live answer")
        )));
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn e10_session_history_failure_is_a_typed_visible_recovery_event() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut subscription_socket, _) =
                listener.accept().await.expect("accept subscription");
            let mut request = vec![0; 4096];
            let _ = subscription_socket
                .read(&mut request)
                .await
                .expect("read subscription request");
            let subscription = serde_json::json!({
                "schema_version": 1,
                "id": "live-test",
                "surface_instance": "tui:test",
                "revision": 1,
                "selector": {"sources": [{
                    "kind": "session",
                    "id": "session-1",
                    "cursor": 0,
                    "detail_scope": "summary"
                }]},
                "selector_hash": "selector-1",
                "expires_at_ms": u64::MAX,
                "stream_url": "/api/runtime/live/live-test"
            })
            .to_string();
            subscription_socket
                .write_all(
                    format!(
                        "HTTP/1.1 201 Created\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{subscription}",
                        subscription.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write subscription response");
            drop(subscription_socket);

            let (mut stream_socket, _) = listener.accept().await.expect("accept stream");
            let mut request = vec![0; 4096];
            let _ = stream_socket
                .read(&mut request)
                .await
                .expect("read stream request");
            let resync = serde_json::json!({
                "schema_version": 1,
                "subscription_id": "live-test",
                "subscription_revision": 1,
                "source_kind": "session",
                "source_id": "session-1",
                "detail_scope": "summary",
                "source_cursor": 0,
                "delivery_class": "snapshot_reconstructable",
                "source_health": "resync_required",
                "event": "source.resync_required",
                "payload": {"reason": "hydrate using durable history"}
            });
            stream_socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\nevent: live\r\ndata: {resync}\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .expect("write stream response");

            for _ in 0..4 {
                let (mut socket, _) = listener.accept().await.expect("accept follow-up request");
                let mut request = vec![0; 4096];
                let size = socket
                    .read(&mut request)
                    .await
                    .expect("read follow-up request");
                let request = String::from_utf8_lossy(&request[..size]);
                if request.starts_with("GET /api/sessions/session-1/history-index") {
                    socket
                        .write_all(
                            b"HTTP/1.1 500 Internal Server Error\r\ncontent-type: application/json\r\ncontent-length: 31\r\nconnection: close\r\n\r\n{\"error\":\"history unavailable\"}",
                        )
                        .await
                        .expect("write history response");
                    break;
                }
                if request.starts_with("DELETE /api/runtime/live-subscriptions/live-test") {
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 11\r\nconnection: close\r\n\r\n{\"ok\":true}",
                        )
                        .await
                        .expect("write delete response");
                    continue;
                }
                panic!("unexpected follow-up request: {request}");
            }
            drop(stream_socket);
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let (tx, mut rx) = crate::cowd_event_channel();
        let progress = client
            .consume_session_live_source(
                "session-1",
                tx.clone(),
                None,
                Arc::new(AtomicUsize::new(0)),
                1,
            )
            .await
            .expect("live stream remains usable while history retries");
        assert_eq!(progress.next_message_sequence, 0);
        let hydration = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .hydrate_session_history("session-1", tx, Arc::new(AtomicUsize::new(0)), 1)
                    .await;
            }
        });
        let failure = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let Ok(event) = rx.try_recv() else {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                };
                if matches!(
                    event,
                    CowdEvent::SessionScoped { ref session_id, ref event, .. }
                        if session_id == "session-1"
                            && matches!(event.as_ref(), CowdEvent::SessionHistoryHydrationFailed {
                                session_id,
                                error
                            } if session_id == "session-1" && error.contains("history unavailable"))
                ) {
                    break event;
                }
            }
        })
        .await
        .expect("typed hydration failure should be visible without timing assumptions");
        assert!(matches!(
            failure,
            CowdEvent::SessionScoped { session_id, .. } if session_id == "session-1"
        ));
        hydration.abort();
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn tui_message_identity_is_reused_for_durable_message_and_idempotency() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept message");
            let mut request = vec![0; 8192];
            let size = socket.read(&mut request).await.expect("read message");
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(
                request.starts_with("POST /api/sessions/session-1/messages HTTP/1.1"),
                "{request}"
            );
            assert!(
                request.contains("\"client_message_id\":\"tui:message-1\""),
                "{request}"
            );
            assert!(
                request.contains("\"idempotency_key\":\"tui:message-1\""),
                "{request}"
            );
            assert!(!request.contains("tui:tui:"), "{request}");
            let body = r#"{"session_id":"session-1","status":"accepted"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let response = client
            .send_message_with_resources("session-1", "hello", &[], Some("tui:message-1"))
            .await
            .expect("send");
        assert_eq!(response["session_id"], "session-1");
        assert_eq!(response["status"], "accepted");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn cowd_projection_gets_surface_contract() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept projection");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read projection");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("GET /api/cowd/projection?surface=tui HTTP/1.1"));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\n\r\n{\"surface\":\"tui\",\"capability_count\":1,\"capabilities\":[]}",
                )
                .await
                .expect("write projection");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let json = client.cowd_projection("tui").await.expect("json");
        assert_eq!(json["surface"], "tui");
        assert_eq!(json["capability_count"], 1);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn gateway_contract_endpoints_get_json_with_auth() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let routes = [
                (
                    "/api/gateway/capability-contract",
                    r#"{"kind":"gateway.capability_contract","capability_count":1,"capabilities":[]}"#,
                ),
                (
                    "/api/gateway/openai-tools",
                    r#"{"kind":"gateway.openai_tools","tool_count":1,"tools":[]}"#,
                ),
            ];
            for (path, body) in routes {
                let (mut socket, _) = listener.accept().await.expect("accept contract");
                let mut buf = vec![0; 2048];
                let n = socket.read(&mut buf).await.expect("read contract");
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(req.starts_with(&format!("GET {path} HTTP/1.1")), "{req}");
                assert!(req.contains("authorization: Bearer test-token"));
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write contract");
            }
        });

        let client =
            GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
                .expect("client");
        let contract = client
            .gateway_capability_contract()
            .await
            .expect("contract");
        let tools = client.gateway_openai_tools().await.expect("tools");
        assert_eq!(contract["kind"], "gateway.capability_contract");
        assert_eq!(tools["kind"], "gateway.openai_tools");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn session_projection_gets_session_run_projection_contract() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept projection");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read projection");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("GET /api/sessions/session%20v31/projection HTTP/1.1"));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\n\r\n{\"kind\":\"session.run_projection\",\"session_id\":\"session v31\"}",
                )
                .await
                .expect("write projection");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let json = client
            .session_projection("session v31")
            .await
            .expect("json");
        assert_eq!(json["kind"], "session.run_projection");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn session_execution_index_uses_the_gateway_contract_route() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept execution index");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read execution index");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("GET /api/sessions/session%20v31/execution HTTP/1.1"));
            let body = r#"{"session_id":"session v31","active_execution_ids":[]}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write execution index");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let index = client
            .session_execution_index("session v31")
            .await
            .expect("execution index");
        assert_eq!(index.session_id, "session v31");
        assert!(index.active_execution_ids.is_empty());
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn structured_projection_gets_all_list_contracts() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let routes = [
                (
                    "/api/cowd/structured/facts",
                    r#"{"kind":"cowd.structured.facts","count":1,"items":[]}"#,
                ),
                (
                    "/api/cowd/structured/evidence",
                    r#"{"kind":"cowd.structured.evidence","count":1,"items":[]}"#,
                ),
                (
                    "/api/cowd/structured/watermarks",
                    r#"{"kind":"cowd.structured.watermarks","count":1,"items":[]}"#,
                ),
            ];
            for (path, body) in routes {
                let (mut socket, _) = listener.accept().await.expect("accept structured");
                let mut buf = vec![0; 2048];
                let n = socket.read(&mut buf).await.expect("read structured");
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    req.starts_with(&format!("GET {path} HTTP/1.1")),
                    "unexpected request: {req}"
                );
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write structured");
            }
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        assert_eq!(
            client.structured_facts().await.expect("facts")["kind"],
            "cowd.structured.facts"
        );
        assert_eq!(
            client.structured_evidence().await.expect("evidence")["kind"],
            "cowd.structured.evidence"
        );
        assert_eq!(
            client.structured_watermarks().await.expect("watermarks")["kind"],
            "cowd.structured.watermarks"
        );
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn reality_projection_gets_status_flow_and_boundaries() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let routes = [
                (
                    "/api/reality/status",
                    r#"{"kind":"reality.status","status":"ready","engines":{}}"#,
                ),
                (
                    "/api/reality/flow?session_id=session-tui",
                    r#"{"kind":"reality.fact_flow","source":"growth.promotions","session_id":"session-tui","stages":[],"events":[],"promotions":[]}"#,
                ),
                (
                    "/api/reality/boundaries",
                    r#"{"kind":"reality.boundaries","boundaries":[]}"#,
                ),
            ];
            for (path, body) in routes {
                let (mut socket, _) = listener.accept().await.expect("accept reality");
                let mut buf = vec![0; 2048];
                let n = socket.read(&mut buf).await.expect("read reality");
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    req.starts_with(&format!("GET {path} HTTP/1.1")),
                    "unexpected request: {req}"
                );
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write reality");
            }
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        assert_eq!(
            client.reality_status().await.expect("status")["kind"],
            "reality.status"
        );
        assert_eq!(
            client
                .reality_flow(Some("session-tui"))
                .await
                .expect("flow")["source"],
            "growth.promotions"
        );
        assert_eq!(
            client.reality_boundaries().await.expect("boundaries")["kind"],
            "reality.boundaries"
        );
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn runtime_session_lease_control_uses_http_routes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept acquire");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read acquire");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/runtime/session-leases/acquire HTTP/1.1"));
            assert!(req.contains("\"session_id\":\"session-1\""));
            assert!(req.contains("\"mode\":\"collaborative\""));
            assert!(req.contains("x-cowd-observer-id:"));
            assert!(!req.contains("\"observer_id\":"));
            assert!(!req.contains("\"owner\":"));
            let body = r#"{"ok":true,"session_id":"session-1","mode":"collaborative"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write acquire");

            let (mut socket, _) = listener.accept().await.expect("accept release");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read release");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/runtime/session-leases/release HTTP/1.1"));
            assert!(req.contains("\"session_id\":\"session-1\""));
            assert!(req.contains("x-cowd-observer-id:"));
            assert!(!req.contains("\"observer_id\":"));
            assert!(!req.contains("\"owner\":"));
            let body = r#"{"ok":true,"session_id":"session-1","released":true}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write release");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let acquired = client
            .acquire_runtime_session_lease("session-1", "collaborative")
            .await
            .expect("acquire");
        assert_eq!(acquired["ok"], true);
        let released = client
            .release_runtime_session_lease("session-1")
            .await
            .expect("release");
        assert_eq!(released["released"], true);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn preflight_cross_plane_action_posts_json() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/cross-plane/action/preflight HTTP/1.1"));
            assert!(req.contains("authorization: Bearer test-token"));
            assert!(req.contains("\"operation\":\"send_text\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 14\r\n\r\n{\"ready\":true}",
                )
                .await
                .expect("write");
        });

        let client =
            GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
                .expect("client");
        let json = client
            .preflight_cross_plane_action(serde_json::json!({
                "operation": "send_text",
                "capability": "surface.webui.send",
            }))
            .await
            .expect("json");
        assert_eq!(json["ready"], true);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn surface_send_posts_gateway_surface_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/surfaces/webui/send HTTP/1.1"));
            assert!(req.contains("\"recipient\":\"user:demo\""));
            assert!(req.contains("\"text\":\"hello\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 39\r\n\r\n{\"kind\":\"surface.result\",\"status\":\"ok\"}",
                )
                .await
                .expect("write");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let json = client
            .surface_send(
                "webui",
                "user:demo",
                None,
                "hello",
                serde_json::json!({"source": "test"}),
            )
            .await
            .expect("json");
        assert_eq!(json["status"], "ok");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn respond_approval_posts_decision() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/approval/respond HTTP/1.1"));
            assert!(req.contains("\"id\":\"approval-1\""));
            assert!(req.contains("\"approved\":true"));
            assert!(req.contains("\"persistence\":\"session\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 17\r\n\r\n{\"resolved\":true}",
                )
                .await
                .expect("write");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let json = client
            .respond_approval("approval-1", true, Some("session"), None)
            .await
            .expect("json");
        assert_eq!(json["resolved"], true);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn connector_resources_gets_search_page() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with(
                "GET /api/connectors/resources?limit=20&offset=40&q=Ready%20Doc HTTP/1.1"
            ));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 16\r\n\r\n{\"resources\":[]}",
                )
                .await
                .expect("write");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let json = client
            .connector_resources(Some("Ready Doc"), 20, 40)
            .await
            .expect("json");
        assert!(json["resources"].as_array().unwrap().is_empty());
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn message_plane_endpoints_use_gateway_routes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let routes = [
                (
                    "GET",
                    "/api/message-connectors",
                    r#"{"kind":"message.connector.registry","connectors":[]}"#,
                ),
                (
                    "GET",
                    "/api/message-connectors/feishu/status",
                    r#"{"kind":"message.connector.status","connector":"feishu"}"#,
                ),
                (
                    "POST",
                    "/api/message-connectors/feishu/repair",
                    r#"{"kind":"message.connector.repair","connector":"feishu"}"#,
                ),
                (
                    "GET",
                    "/api/message-endpoints",
                    r#"{"kind":"message.endpoint.directory","endpoints":[]}"#,
                ),
                (
                    "GET",
                    "/api/message-routes",
                    r#"{"kind":"message.delivery.routes","routes":[]}"#,
                ),
                (
                    "GET",
                    "/api/message-bindings",
                    r#"{"kind":"message.conversation.bindings","bindings":[]}"#,
                ),
            ];
            for (method, path, body) in routes {
                let (mut socket, _) = listener.accept().await.expect("accept message plane");
                let mut buf = vec![0; 2048];
                let n = socket.read(&mut buf).await.expect("read message plane");
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    req.starts_with(&format!("{method} {path} HTTP/1.1")),
                    "unexpected request: {req}"
                );
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write message plane");
            }
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        assert_eq!(
            client.message_connectors().await.expect("connectors")["kind"],
            "message.connector.registry"
        );
        assert_eq!(
            client
                .message_connector_status("feishu")
                .await
                .expect("status")["kind"],
            "message.connector.status"
        );
        assert_eq!(
            client
                .message_connector_repair("feishu")
                .await
                .expect("repair")["kind"],
            "message.connector.repair"
        );
        assert_eq!(
            client.message_endpoints().await.expect("endpoints")["kind"],
            "message.endpoint.directory"
        );
        assert_eq!(
            client.message_routes().await.expect("routes")["kind"],
            "message.delivery.routes"
        );
        assert_eq!(
            client.message_bindings().await.expect("bindings")["kind"],
            "message.conversation.bindings"
        );
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn connector_service_tools_and_execute_use_management_routes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept tools");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read tools");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("GET /api/connectors/services/local.docs/tools HTTP/1.1"));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 12\r\n\r\n{\"tools\":[]}",
                )
                .await
                .expect("write tools");

            let (mut socket, _) = listener.accept().await.expect("accept execute");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read execute");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/connectors/services/local.docs/execute HTTP/1.1"));
            assert!(req.contains("\"tool_id\":\"service.local.docs.read\""));
            assert!(req.contains("\"mode\":\"dry_run\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write execute");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let tools = client
            .connector_service_tools("local.docs")
            .await
            .expect("tools");
        assert!(tools["tools"].as_array().unwrap().is_empty());
        let result = client
            .execute_connector_service(
                "local.docs",
                serde_json::json!({
                    "actor_principal": "tui:operator",
                    "tool_id": "service.local.docs.read",
                    "resource_id": "tui-doc",
                    "title": "TUI Doc",
                    "mode": "dry_run",
                }),
            )
            .await
            .expect("execute");
        assert_eq!(result["ok"], true);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn tool_operations_routes_use_management_contract() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let checks: Vec<(&str, &str, Vec<&str>)> = vec![
                ("GET", "/api/tools", vec![]),
                (
                    "POST",
                    "/api/tools/execute",
                    vec!["\"name\":\"tool_cache_stats\"", "\"mode\":\"read_only\""],
                ),
                ("GET", "/api/tools/cache", vec![]),
                (
                    "POST",
                    "/api/tools/batch-readonly",
                    vec!["\"max_concurrency\":3", "\"name\":\"tool_cache_stats\""],
                ),
                (
                    "POST",
                    "/api/tools/mutations/preview",
                    vec!["\"path\":\"README.md\""],
                ),
                (
                    "POST",
                    "/api/tools/mutations/apply",
                    vec!["\"expected_hashes\"", "\"README.md\":\"hash-1\""],
                ),
                ("GET", "/api/tools/checkpoints", vec![]),
                (
                    "POST",
                    "/api/tools/checkpoints",
                    vec!["\"label\":\"before edit\""],
                ),
                ("GET", "/api/tools/checkpoints/cp-1/diff", vec![]),
                ("POST", "/api/tools/checkpoints/cp-1/restore", vec![]),
                (
                    "POST",
                    "/api/tools/intent-plan",
                    vec!["\"prompt\":\"inspect\"", "\"selected_tools\""],
                ),
                (
                    "POST",
                    "/api/tools/context-fanout/plan",
                    vec!["\"prompt\":\"fanout\""],
                ),
                (
                    "GET",
                    "/api/runtime/timeline?session_id=session%20a%2Fb&limit=25",
                    vec![],
                ),
                (
                    "POST",
                    "/api/cross-plane/policy/simulate",
                    vec!["\"requested_capability\":\"service.read\""],
                ),
            ];

            for (method, path, needles) in checks {
                let (mut socket, _) = listener.accept().await.expect("accept tool ops");
                let mut buf = vec![0; 8192];
                let n = socket.read(&mut buf).await.expect("read tool ops");
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    req.starts_with(&format!("{method} {path} HTTP/1.1")),
                    "unexpected request for {method} {path}: {req}"
                );
                for needle in needles {
                    assert!(req.contains(needle), "missing `{needle}` in request: {req}");
                }
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                    )
                    .await
                    .expect("write tool ops");
            }
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        assert_eq!(client.tool_registry().await.expect("registry")["ok"], true);
        assert_eq!(
            client
                .tool_execute("tool_cache_stats", serde_json::json!({}), "read_only")
                .await
                .expect("execute")["ok"],
            true
        );
        assert_eq!(client.tool_cache_stats().await.expect("cache")["ok"], true);
        assert_eq!(
            client
                .tool_batch_readonly(
                    vec![serde_json::json!({ "name": "tool_cache_stats", "input": {} })],
                    3,
                )
                .await
                .expect("batch")["ok"],
            true
        );
        let edits = vec![serde_json::json!({
            "path": "README.md",
            "old_string": "A",
            "new_string": "B"
        })];
        assert_eq!(
            client
                .tool_mutation_preview(edits.clone())
                .await
                .expect("preview")["ok"],
            true
        );
        assert_eq!(
            client
                .tool_mutation_apply(edits, serde_json::json!({ "README.md": "hash-1" }))
                .await
                .expect("apply")["ok"],
            true
        );
        assert_eq!(
            client.tool_checkpoints().await.expect("checkpoints")["ok"],
            true
        );
        assert_eq!(
            client
                .tool_checkpoint_create("before edit")
                .await
                .expect("checkpoint create")["ok"],
            true
        );
        assert_eq!(
            client
                .tool_checkpoint_diff("cp-1")
                .await
                .expect("checkpoint diff")["ok"],
            true
        );
        assert_eq!(
            client
                .tool_checkpoint_restore("cp-1")
                .await
                .expect("checkpoint restore")["ok"],
            true
        );
        assert_eq!(
            client
                .tool_intent_plan("inspect", vec!["tool_cache_stats".to_string()])
                .await
                .expect("intent")["ok"],
            true
        );
        assert_eq!(
            client
                .tool_context_fanout_plan("fanout")
                .await
                .expect("fanout")["ok"],
            true
        );
        assert_eq!(
            client
                .runtime_timeline("session a/b", 25)
                .await
                .expect("timeline")["ok"],
            true
        );
        assert_eq!(
            client
                .cross_plane_policy_simulate(serde_json::json!({
                    "requested_capability": "service.read"
                }))
                .await
                .expect("policy simulate")["ok"],
            true
        );
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn connector_resource_lifecycle_routes_use_management_contract() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept revalidate");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read revalidate");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/connectors/resources/revalidate HTTP/1.1"));
            assert!(req.contains("\"reference\":\"service://local.docs/document/tui-doc\""));
            assert!(req.contains("\"state\":\"stale\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write revalidate");

            let (mut socket, _) = listener.accept().await.expect("accept promote");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read promote");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/connectors/resources/promote-memory HTTP/1.1"));
            assert!(req.contains("\"reference\":\"service://local.docs/document/tui-doc\""));
            assert!(req.contains("\"session_id\":\"session-tui\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write promote");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let revalidated = client
            .revalidate_connector_resource("service://local.docs/document/tui-doc", "stale")
            .await
            .expect("revalidate");
        assert_eq!(revalidated["ok"], true);
        let promoted = client
            .promote_connector_resource_to_memory(
                "service://local.docs/document/tui-doc",
                Some("session-tui"),
            )
            .await
            .expect("promote");
        assert_eq!(promoted["ok"], true);
        server.await.expect("server task");
    }
}
