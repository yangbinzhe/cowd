use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    convert::Infallible,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Extension, Path, Query, State as AxumState},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, patch, post},
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use futures::{Stream, StreamExt};
use harness_contract::live::{
    CompositeCheckpoint, CreateLiveSubscriptionRequest, DeliveryClass, LiveEnvelope, LiveSelector,
    LiveSourceKind, LiveSourceSelector, LiveSubscription, PatchLiveSubscriptionRequest,
    SourceHealth, LIVE_CONTRACT_SCHEMA_VERSION,
};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{mpsc, oneshot, watch, Mutex as AsyncMutex, RwLock},
    task::JoinSet,
};
use tokio_stream::wrappers::ReceiverStream;

use super::{
    message_routes, runtime_routes,
    session_routes::{authorize_session_access, SessionAccess},
    AppState, AuthenticatedPrincipal, ErrorResponse,
};

const MAX_SOURCE_ID_BYTES: usize = 256;
const MAX_SURFACE_INSTANCE_BYTES: usize = 128;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
const MAX_PENDING_TEXT_PREVIEW_BYTES: usize = 1024 * 1024;
const CHECKPOINT_KEY_ROTATION_MS: u64 = 6 * 60 * 60 * 1_000;
const SURFACE_INSTANCE_HEADER: &str = "x-cowd-observer-id";

type HmacSha256 = Hmac<Sha256>;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/api/runtime/live-subscriptions",
            post(create_live_subscription),
        )
        .route(
            "/api/runtime/live-subscriptions/:id",
            patch(patch_live_subscription).delete(delete_live_subscription),
        )
        .route("/api/runtime/live/:id", get(get_live_stream))
}

#[derive(Debug, Clone)]
struct SubscriptionRevision {
    revision: u64,
    selector: LiveSelector,
    selector_hash: String,
    expires_at_ms: u64,
    deleted: bool,
}

struct SubscriptionEntry {
    id: String,
    principal_binding: String,
    principal_hash: String,
    surface_instance: String,
    surface_instance_hash: String,
    idempotency_key: Option<String>,
    create_request_hash: String,
    limits: runtime::GatewayLiveConfig,
    patch_lock: AsyncMutex<()>,
    patch_idempotency: Mutex<HashMap<String, PatchReceipt>>,
    pending_previews: Mutex<BTreeMap<String, QueuedEnvelope>>,
    revisions: watch::Sender<Arc<SubscriptionRevision>>,
    active_connection: AtomicBool,
}

#[derive(Clone)]
struct PatchReceipt {
    request_hash: String,
    subscription: LiveSubscription,
}

impl SubscriptionEntry {
    fn snapshot(&self) -> Arc<SubscriptionRevision> {
        self.revisions.borrow().clone()
    }

    fn public(&self) -> LiveSubscription {
        let snapshot = self.snapshot();
        LiveSubscription {
            schema_version: LIVE_CONTRACT_SCHEMA_VERSION,
            id: self.id.clone(),
            surface_instance: self.surface_instance.clone(),
            revision: snapshot.revision,
            selector: snapshot.selector.clone(),
            selector_hash: snapshot.selector_hash.clone(),
            expires_at_ms: snapshot.expires_at_ms,
            stream_url: format!(
                "/api/runtime/live/{}?surface_instance={}",
                self.id, self.surface_instance
            ),
        }
    }
}

pub(crate) struct LiveRegistry {
    subscriptions: RwLock<HashMap<String, Arc<SubscriptionEntry>>>,
    checkpoint_secret: [u8; 32],
}

impl LiveRegistry {
    pub(crate) fn new() -> Self {
        let mut hasher = Sha256::new();
        hasher.update(uuid::Uuid::new_v4().as_bytes());
        hasher.update(uuid::Uuid::new_v4().as_bytes());
        hasher.update(now_ms().to_le_bytes());
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            checkpoint_secret: hasher.finalize().into(),
        }
    }
}

async fn create_live_subscription(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(request): Json<CreateLiveSubscriptionRequest>,
) -> Result<(StatusCode, Json<LiveSubscription>), (StatusCode, Json<ErrorResponse>)> {
    let surface_instance = validate_surface_instance(&request.surface_instance)?;
    require_surface_instance(&headers, Some(&surface_instance))?;
    let idempotency_key = validate_idempotency_key(request.idempotency_key.as_deref())?;
    let limits = gateway_live_config(&state);
    let selector = normalize_selector(request.selector, limits.max_sources)?;
    validate_selector_authority(&state, &principal, &selector).await?;
    let principal_binding = principal_binding(&principal);
    let expires_at_ms = expiry_from_ttl(request.ttl_seconds, &limits)?;
    let request_hash = create_request_hash(
        &surface_instance,
        &selector,
        request.ttl_seconds.unwrap_or(limits.default_ttl_seconds),
    );

    let registry = &state.live_registry;
    let mut subscriptions = registry.subscriptions.write().await;
    subscriptions.retain(|_, entry| {
        let snapshot = entry.snapshot();
        !snapshot.deleted && snapshot.expires_at_ms > now_ms()
    });
    if let Some(key) = idempotency_key.as_deref() {
        if let Some(existing) = subscriptions.values().find(|entry| {
            entry.principal_binding == principal_binding
                && entry.surface_instance == surface_instance
                && entry.idempotency_key.as_deref() == Some(key)
        }) {
            if existing.create_request_hash == request_hash {
                return Ok((StatusCode::OK, Json(existing.public())));
            }
            return Err(api_error(
                StatusCode::CONFLICT,
                "live subscription idempotency key was reused for a different POST",
            ));
        }
    }
    let active_for_instance = subscriptions
        .values()
        .filter(|entry| {
            entry.principal_binding == principal_binding
                && entry.surface_instance == surface_instance
        })
        .count();
    if active_for_instance >= limits.max_subscriptions_per_principal_instance {
        return Err(api_error(
            StatusCode::TOO_MANY_REQUESTS,
            "live subscription count exceeded for this principal and Surface instance",
        ));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let selector_hash = selector_hash(&selector);
    let revision = Arc::new(SubscriptionRevision {
        revision: 1,
        selector,
        selector_hash,
        expires_at_ms,
        deleted: false,
    });
    let (revision_tx, _) = watch::channel(revision);
    let entry = Arc::new(SubscriptionEntry {
        id: id.clone(),
        principal_hash: hash_text(&principal_binding),
        principal_binding,
        surface_instance_hash: hash_text(&surface_instance),
        surface_instance,
        idempotency_key,
        create_request_hash: request_hash,
        limits,
        patch_lock: AsyncMutex::new(()),
        patch_idempotency: Mutex::new(HashMap::new()),
        pending_previews: Mutex::new(BTreeMap::new()),
        revisions: revision_tx,
        active_connection: AtomicBool::new(false),
    });
    let response = entry.public();
    subscriptions.insert(id, entry);
    Ok((StatusCode::CREATED, Json(response)))
}

async fn patch_live_subscription(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<PatchLiveSubscriptionRequest>,
) -> Result<Json<LiveSubscription>, (StatusCode, Json<ErrorResponse>)> {
    let idempotency_key = validate_required_idempotency_key(&request.idempotency_key)?;
    let entry = find_owned_subscription(
        &state.live_registry,
        &principal,
        &id,
        require_surface_instance(&headers, None)?.as_str(),
    )
    .await?;
    let selector = normalize_selector(request.selector, entry.limits.max_sources)?;
    validate_selector_authority(&state, &principal, &selector).await?;
    let request_hash =
        patch_request_hash(request.expected_revision, &selector, request.ttl_seconds);
    patch_live_subscription_entry(
        &entry,
        idempotency_key,
        request.expected_revision,
        selector,
        request.ttl_seconds,
        request_hash,
    )
    .await
}

async fn patch_live_subscription_entry(
    entry: &Arc<SubscriptionEntry>,
    idempotency_key: String,
    expected_revision: u64,
    selector: LiveSelector,
    ttl_seconds: Option<u64>,
    request_hash: String,
) -> Result<Json<LiveSubscription>, (StatusCode, Json<ErrorResponse>)> {
    let _patch_guard = entry.patch_lock.lock().await;
    if let Some(receipt) = entry
        .patch_idempotency
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&idempotency_key)
        .cloned()
    {
        if receipt.request_hash == request_hash {
            return Ok(Json(receipt.subscription));
        }
        return Err(api_error(
            StatusCode::CONFLICT,
            "live subscription idempotency key was reused for a different PATCH",
        ));
    }
    let current = entry.snapshot();
    if current.deleted || current.expires_at_ms <= now_ms() {
        return Err(api_error(StatusCode::GONE, "live subscription expired"));
    }
    if current.revision != expected_revision {
        return Err(api_error(
            StatusCode::CONFLICT,
            format!(
                "live subscription revision mismatch: expected {}, current {}",
                expected_revision, current.revision
            ),
        ));
    }
    let expires_at_ms = match ttl_seconds {
        Some(ttl) => expiry_from_ttl(Some(ttl), &entry.limits)?,
        None => current.expires_at_ms,
    };
    entry
        .pending_previews
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    entry.revisions.send_replace(Arc::new(SubscriptionRevision {
        revision: current.revision.saturating_add(1),
        selector_hash: selector_hash(&selector),
        selector,
        expires_at_ms,
        deleted: false,
    }));
    let response = entry.public();
    entry
        .patch_idempotency
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            idempotency_key,
            PatchReceipt {
                request_hash,
                subscription: response.clone(),
            },
        );
    Ok(Json(response))
}

async fn delete_live_subscription(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let entry = find_owned_subscription(
        &state.live_registry,
        &principal,
        &id,
        require_surface_instance(&headers, None)?.as_str(),
    )
    .await?;
    let current = entry.snapshot();
    entry
        .pending_previews
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    entry.revisions.send_replace(Arc::new(SubscriptionRevision {
        revision: current.revision.saturating_add(1),
        selector: LiveSelector::default(),
        selector_hash: selector_hash(&LiveSelector::default()),
        expires_at_ms: now_ms(),
        deleted: true,
    }));
    state.live_registry.subscriptions.write().await.remove(&id);
    if let Some(runtime) = state.services.runtime.as_ref() {
        runtime
            .gateway_tasks()
            .close_owner_and_drain(
                crate::runtime_host::task_set::GatewayTaskOwner::LiveSubscription(id),
                std::time::Duration::from_secs(5),
            )
            .await;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn get_live_stream(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let surface_instance =
        require_surface_instance(&headers, query.get("surface_instance").map(String::as_str))?;
    let entry =
        find_owned_subscription(&state.live_registry, &principal, &id, &surface_instance).await?;
    let snapshot = entry.snapshot();
    if snapshot.deleted || snapshot.expires_at_ms <= now_ms() {
        return Err(api_error(StatusCode::GONE, "live subscription expired"));
    }
    if entry
        .active_connection
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(api_error(
            StatusCode::CONFLICT,
            "live subscription already has an active physical connection",
        ));
    }

    let (initial_cursors, initial_revisions) = match headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
    {
        Some(token) => match verify_checkpoint(
            token,
            &entry,
            &snapshot,
            &state.live_registry.checkpoint_secret,
        ) {
            Ok(checkpoint) => (checkpoint.source_cursors, checkpoint.source_revisions),
            Err(error) => {
                entry.active_connection.store(false, Ordering::Release);
                return Err(api_error(StatusCode::CONFLICT, error));
            }
        },
        None => (
            snapshot
                .selector
                .sources
                .iter()
                .map(|source| (source.key(), source.cursor))
                .collect(),
            snapshot
                .selector
                .sources
                .iter()
                .map(|source| (source.key(), source.revision))
                .collect(),
        ),
    };

    let (tx, rx) = mpsc::channel(entry.limits.queue_capacity);
    let terminal = Arc::new(Mutex::new(None));
    let delivered_cursors = Arc::new(Mutex::new(initial_cursors.clone()));
    let delivered_revisions = Arc::new(Mutex::new(initial_revisions.clone()));
    let checkpoint_secret = state.live_registry.checkpoint_secret;
    if let Err(error) = spawn_subscription_coordinator(
        Arc::clone(&state),
        principal,
        Arc::clone(&entry),
        tx,
        Arc::clone(&terminal),
        initial_cursors,
        initial_revisions,
    ) {
        entry.active_connection.store(false, Ordering::Release);
        return Err(api_error(StatusCode::SERVICE_UNAVAILABLE, error));
    }
    let stream = PhysicalLiveStream {
        rx: ReceiverStream::new(rx),
        entry,
        terminal,
        delivered_cursors,
        delivered_revisions,
        checkpoint_secret,
        ready_revision: 0,
        ended: false,
    };
    Ok(Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(std::time::Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response())
}

async fn find_owned_subscription(
    registry: &LiveRegistry,
    principal: &AuthenticatedPrincipal,
    id: &str,
    surface_instance: &str,
) -> Result<Arc<SubscriptionEntry>, (StatusCode, Json<ErrorResponse>)> {
    let entry = registry
        .subscriptions
        .read()
        .await
        .get(id)
        .cloned()
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "live subscription not found"))?;
    if entry.principal_binding != principal_binding(principal) {
        return Err(api_error(
            StatusCode::FORBIDDEN,
            "live subscription belongs to a different authenticated principal",
        ));
    }
    if entry.surface_instance != surface_instance {
        return Err(api_error(
            StatusCode::FORBIDDEN,
            "live subscription belongs to a different Surface instance",
        ));
    }
    Ok(entry)
}

fn spawn_subscription_coordinator(
    state: Arc<AppState>,
    principal: AuthenticatedPrincipal,
    entry: Arc<SubscriptionEntry>,
    tx: mpsc::Sender<QueuedEnvelope>,
    terminal: Arc<Mutex<Option<TerminalSignal>>>,
    initial_cursors: BTreeMap<String, u64>,
    initial_revisions: BTreeMap<String, u64>,
) -> Result<(), String> {
    let tasks = state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.gateway_tasks())
        .ok_or_else(|| "Runtime task owner is unavailable".to_string())?;
    let subscription_id = entry.id.clone();
    tasks
        .spawn_owned(
            crate::runtime_host::task_set::GatewayTaskKind::LiveSubscription,
            crate::runtime_host::task_set::GatewayTaskOwner::LiveSubscription(
                subscription_id.clone(),
            ),
            move |cancellation| async move {
                run_subscription_coordinator(
                    state,
                    principal,
                    entry,
                    tx,
                    terminal,
                    initial_cursors,
                    initial_revisions,
                    cancellation,
                )
                .await;
            },
        )
        .map(|_| ())
        .map_err(|error| {
            format!("live subscription `{subscription_id}` task admission failed: {error}")
        })
}

async fn run_subscription_coordinator(
    state: Arc<AppState>,
    principal: AuthenticatedPrincipal,
    entry: Arc<SubscriptionEntry>,
    tx: mpsc::Sender<QueuedEnvelope>,
    terminal: Arc<Mutex<Option<TerminalSignal>>>,
    initial_cursors: BTreeMap<String, u64>,
    initial_revisions: BTreeMap<String, u64>,
    cancellation: runtime::CancellationToken,
) {
    let mut revision_rx = entry.revisions.subscribe();
    let mut previous_sources = BTreeSet::new();
    let mut first_revision = true;
    let mut retiring_source_tasks: Option<JoinSet<()>> = None;
    loop {
        let revision = revision_rx.borrow().clone();
        if revision.deleted || revision.expires_at_ms <= now_ms() {
            signal_terminal(
                &tx,
                &terminal,
                "subscription.closed",
                "live subscription was deleted or expired",
            );
            return;
        }
        let current_sources = revision
            .selector
            .sources
            .iter()
            .map(LiveSourceSelector::key)
            .collect::<BTreeSet<_>>();
        if !first_revision {
            for removed in previous_sources.difference(&current_sources) {
                let (source_kind, source_id) = split_source_key(removed);
                let revoked = envelope(
                    &entry,
                    revision.revision,
                    source_kind,
                    source_id,
                    harness_contract::projection::ProjectionDetailScope::Summary,
                    None,
                    DeliveryClass::SnapshotReconstructable,
                    SourceHealth::Revoked,
                    "source.revoked",
                    serde_json::json!({"reason": "selector_removed"}),
                );
                queue_envelope(&tx, &terminal, &entry, revoked, None);
            }
        }
        let mut source_tasks = JoinSet::new();
        let mut baseline_receivers = Vec::new();
        let (release_tx, release_rx) = watch::channel(false);
        for mut source in revision.selector.sources.clone() {
            let (baseline_tx, baseline_rx) = oneshot::channel();
            baseline_receivers.push(baseline_rx);
            let source_cursor = if first_revision {
                initial_cursors
                    .get(&source.key())
                    .copied()
                    .unwrap_or(source.cursor)
            } else {
                source.cursor
            };
            if first_revision {
                source.revision = initial_revisions
                    .get(&source.key())
                    .copied()
                    .unwrap_or(source.revision);
            }
            let source_state = Arc::clone(&state);
            let source_principal = principal.clone();
            let source_entry = Arc::clone(&entry);
            let source_tx = tx.clone();
            let source_terminal = Arc::clone(&terminal);
            let source_revision = revision.revision;
            let source_release = release_rx.clone();
            source_tasks.spawn(async move {
                match source.kind {
                    LiveSourceKind::Session => {
                        run_session_source(
                            source_state,
                            source_principal,
                            source_entry,
                            source_tx,
                            source_terminal,
                            source,
                            source_revision,
                            source_cursor,
                            baseline_tx,
                            source_release,
                        )
                        .await;
                    }
                    LiveSourceKind::Execution => {
                        run_execution_source(
                            source_state,
                            source_principal,
                            source_entry,
                            source_tx,
                            source_terminal,
                            source,
                            source_revision,
                            source_cursor,
                            baseline_tx,
                            source_release,
                        )
                        .await;
                    }
                    LiveSourceKind::Mission => {
                        run_mission_source(
                            source_state,
                            source_principal,
                            source_entry,
                            source_tx,
                            source_terminal,
                            source,
                            source_revision,
                            source_cursor,
                            baseline_tx,
                            source_release,
                        )
                        .await;
                    }
                }
            });
        }

        let baselines_ready = tokio::select! {
            _ = cancellation.cancelled() => {
                abort_and_join_live_sources(&mut source_tasks).await;
                if let Some(mut retiring) = retiring_source_tasks.take() {
                    abort_and_join_live_sources(&mut retiring).await;
                }
                return;
            }
            ready = tokio::time::timeout(
                std::time::Duration::from_millis(entry.limits.baseline_timeout_ms),
                futures::future::join_all(baseline_receivers),
            ) => ready,
        };
        if !matches!(
            baselines_ready,
            Ok(ref results) if results.iter().all(|result| result.is_ok())
        ) {
            signal_terminal(
                &tx,
                &terminal,
                "subscription.baseline_timeout",
                "live subscription source baselines did not materialize in time",
            );
            abort_and_join_live_sources(&mut source_tasks).await;
            if let Some(mut retiring) = retiring_source_tasks.take() {
                abort_and_join_live_sources(&mut retiring).await;
            }
            return;
        }
        let barrier_event = if first_revision {
            "subscription.ready"
        } else {
            "subscription.revision.changed"
        };
        let barrier = envelope(
            &entry,
            revision.revision,
            "subscription",
            &entry.id,
            harness_contract::projection::ProjectionDetailScope::Summary,
            None,
            DeliveryClass::SnapshotReconstructable,
            SourceHealth::Baseline,
            barrier_event,
            serde_json::json!({
                "revision": revision.revision,
                "selector_hash": revision.selector_hash,
                "source_count": revision.selector.sources.len(),
            }),
        );
        queue_envelope(&tx, &terminal, &entry, barrier, None);
        release_tx.send_replace(true);
        if let Some(mut retiring) = retiring_source_tasks.take() {
            abort_and_join_live_sources(&mut retiring).await;
        }

        previous_sources = current_sources;
        first_revision = false;
        let mut auth_interval = tokio::time::interval(std::time::Duration::from_secs(1));
        auth_interval.tick().await;
        let transition = loop {
            tokio::select! {
                _ = cancellation.cancelled() => break false,
                changed = revision_rx.changed() => break changed.is_ok(),
                _ = tx.closed() => break false,
                _ = auth_interval.tick() => {
                    if revision.expires_at_ms <= now_ms() {
                        signal_terminal(&tx, &terminal, "subscription.expired", "live subscription TTL expired");
                        break false;
                    }
                    let config_home = state.config_home.clone();
                    let principal = principal.clone();
                    let current = tokio::task::spawn_blocking(move || {
                        super::projection_stream_principal_current(&config_home, &principal)
                    }).await;
                    if !matches!(current, Ok(Ok(()))) {
                        signal_terminal(
                            &tx,
                            &terminal,
                            "subscription.authorization_revoked",
                            "authenticated principal is no longer current",
                        );
                        break false;
                    }
                }
            }
        };
        if !transition {
            abort_and_join_live_sources(&mut source_tasks).await;
            return;
        }
        // Keep the old revision alive until every source in the replacement
        // revision has subscribed, materialized its baseline and crossed the
        // revision barrier. The client already ignores old-revision
        // envelopes, while this overlap closes the event-bus gap that would
        // otherwise lose non-replayable live deltas during selector PATCHes.
        retiring_source_tasks = Some(source_tasks);
    }
}

async fn abort_and_join_live_sources(tasks: &mut JoinSet<()>) {
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            tracing::debug!(%error, "live source task joined after cancellation");
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_session_source(
    state: Arc<AppState>,
    principal: AuthenticatedPrincipal,
    entry: Arc<SubscriptionEntry>,
    tx: mpsc::Sender<QueuedEnvelope>,
    terminal: Arc<Mutex<Option<TerminalSignal>>>,
    source: LiveSourceSelector,
    revision: u64,
    mut cursor: u64,
    baseline_ready: oneshot::Sender<()>,
    mut release: watch::Receiver<bool>,
) {
    let mut baseline_ready = Some(baseline_ready);
    if let Err((status, error)) =
        authorize_session_access(&state, &principal, &source.id, SessionAccess::Read).await
    {
        queue_source_revoke(
            &tx,
            &terminal,
            &entry,
            revision,
            &source,
            format!("{status}: {}", error.error),
        );
        mark_source_baseline_ready(&mut baseline_ready);
        return;
    }

    let event_bus = state.event_bus();
    let mut subscription = event_bus.subscribe(&source.id, 256).await;
    let subscription_id = subscription.id();

    const PAGE_SIZE: usize = 500;
    const MAX_PAGES: usize = 100;
    for _ in 0..MAX_PAGES {
        let page = message_routes::replay_materialized_terminal_events(
            &state, &source.id, cursor, PAGE_SIZE,
        )
        .await;
        for raw in page.events {
            let payload = serde_json::from_str(&raw).unwrap_or_else(|_| {
                serde_json::json!({"type": "session_stream_resync", "reason": "invalid_replay"})
            });
            let event_cursor = message_routes::stream_durable_cursor(&raw);
            if let Some(value) = event_cursor {
                if value <= cursor {
                    continue;
                }
                cursor = value;
            }
            queue_session_payload(
                &tx,
                &terminal,
                &entry,
                revision,
                &source,
                payload,
                event_cursor,
            );
        }
        if page.requires_resync {
            break;
        }
        cursor = cursor.max(page.last_cursor.unwrap_or(cursor));
        if page.record_count < PAGE_SIZE {
            break;
        }
    }

    let baseline = envelope(
        &entry,
        revision,
        "session",
        &source.id,
        source.detail_scope,
        Some(cursor),
        DeliveryClass::SnapshotReconstructable,
        SourceHealth::Baseline,
        "session.connected",
        serde_json::json!({
            "type": "Connected",
            "session_id": source.id,
            "runtime_commit_cursor": cursor,
        }),
    );
    queue_envelope(
        &tx,
        &terminal,
        &entry,
        baseline,
        Some((source.key(), cursor, 0)),
    );
    mark_source_baseline_ready(&mut baseline_ready);
    if !await_revision_release(&mut release).await {
        event_bus.unsubscribe(&source.id, subscription_id).await;
        return;
    }

    loop {
        tokio::select! {
            _ = tx.closed() => break,
            payload = subscription.recv() => {
                let Some(event) = payload else { break; };
                let payload = event.to_transport_value();
                if authorize_session_access(&state, &principal, &source.id, SessionAccess::Read).await.is_err() {
                    let attachment_actor =
                        super::surface_actor_id(&principal, &entry.surface_instance);
                    let lease_owner =
                        super::session_lease_owner(&principal, &entry.surface_instance);
                    message_routes::cleanup_revoked_session_stream_authority(
                        &state,
                        &source.id,
                        Some(&attachment_actor),
                        &lease_owner,
                    )
                    .await;
                    queue_source_revoke(
                        &tx,
                        &terminal,
                        &entry,
                        revision,
                        &source,
                        "session scope is no longer authorized".to_string(),
                    );
                    break;
                }
                let event_cursor = message_routes::stream_durable_cursor_value(&payload);
                if event_cursor.is_some_and(|value| value <= cursor) {
                    continue;
                }
                if let Some(value) = event_cursor {
                    cursor = value;
                }
                queue_session_payload(
                    &tx,
                    &terminal,
                    &entry,
                    revision,
                    &source,
                    payload,
                    event_cursor,
                );
            }
        }
    }
    event_bus.unsubscribe(&source.id, subscription_id).await;
}

#[allow(clippy::too_many_arguments)]
async fn run_execution_source(
    state: Arc<AppState>,
    principal: AuthenticatedPrincipal,
    entry: Arc<SubscriptionEntry>,
    tx: mpsc::Sender<QueuedEnvelope>,
    terminal: Arc<Mutex<Option<TerminalSignal>>>,
    source: LiveSourceSelector,
    revision: u64,
    mut cursor: u64,
    baseline_ready: oneshot::Sender<()>,
    mut release: watch::Receiver<bool>,
) {
    let mut baseline_ready = Some(baseline_ready);
    let runtime = match runtime_routes::execution_runtime(&state) {
        Ok(runtime) => runtime,
        Err((_, error)) => {
            queue_source_resync(
                &tx,
                &terminal,
                &entry,
                revision,
                &source,
                cursor,
                &error.error,
            );
            mark_source_baseline_ready(&mut baseline_ready);
            return;
        }
    };
    let initial_context = match runtime_routes::execution_projection_context(
        &state,
        &principal,
        &source.id,
        source.detail_scope,
    )
    .await
    {
        Ok(context) => context,
        Err((status, error))
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN =>
        {
            queue_source_revoke(
                &tx,
                &terminal,
                &entry,
                revision,
                &source,
                error.error.clone(),
            );
            mark_source_baseline_ready(&mut baseline_ready);
            return;
        }
        Err((_, error)) => {
            queue_source_resync(
                &tx,
                &terminal,
                &entry,
                revision,
                &source,
                cursor,
                &error.error,
            );
            mark_source_baseline_ready(&mut baseline_ready);
            return;
        }
    };
    let (
        mut projection_revision,
        mut projection_authorization_revision,
        mut projection_redaction_revision,
        mut last_live_revision,
        initial_terminal,
    ) = if cursor > 0 && source.revision > 0 {
        let delta = match runtime::execution_projection::delta(
            &runtime,
            &source.id,
            source.revision,
            cursor,
            &initial_context,
        ) {
            Ok(delta) if delta.resync_reason.is_none() => delta,
            Ok(delta) => {
                queue_source_resync(
                    &tx,
                    &terminal,
                    &entry,
                    revision,
                    &source,
                    cursor,
                    &format!(
                        "execution projection resume requires resync: {:?}",
                        delta.resync_reason
                    ),
                );
                mark_source_baseline_ready(&mut baseline_ready);
                return;
            }
            Err(error) => {
                queue_source_resync(
                    &tx,
                    &terminal,
                    &entry,
                    revision,
                    &source,
                    cursor,
                    &error.to_string(),
                );
                mark_source_baseline_ready(&mut baseline_ready);
                return;
            }
        };
        let terminal_delta = delta.operations.iter().any(|operation| {
            matches!(
                operation,
                harness_contract::projection::ProjectionOperation::SetTerminal { .. }
                    | harness_contract::projection::ProjectionOperation::SetDeliveryTruth { .. }
            )
        });
        cursor = delta.target_cursor;
        if delta.target_cursor > delta.base_cursor || delta.target_revision > delta.from_revision {
            let update = envelope(
                &entry,
                revision,
                "execution",
                &source.id,
                source.detail_scope,
                Some(cursor),
                DeliveryClass::Durable,
                SourceHealth::Live,
                "projection_delta",
                serde_json::to_value(&delta).unwrap_or_default(),
            );
            queue_envelope(
                &tx,
                &terminal,
                &entry,
                update,
                Some((source.key(), cursor, delta.target_revision)),
            );
        }
        let resumed_live = runtime.execution_live(&source.id);
        (
            delta.target_revision,
            delta.authorization_revision,
            delta.redaction_revision,
            resumed_live.as_ref().map(|live| live.revision),
            terminal_delta
                || resumed_live
                    .as_ref()
                    .is_some_and(|live| live.status.is_terminal()),
        )
    } else {
        let initial_snapshot =
            match runtime::execution_projection::snapshot(&runtime, &source.id, &initial_context)
                .await
            {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    queue_source_resync(
                        &tx,
                        &terminal,
                        &entry,
                        revision,
                        &source,
                        cursor,
                        &error.to_string(),
                    );
                    mark_source_baseline_ready(&mut baseline_ready);
                    return;
                }
            };
        if initial_snapshot.cursor < cursor {
            queue_source_resync(
                &tx,
                &terminal,
                &entry,
                revision,
                &source,
                cursor,
                "execution checkpoint is ahead of the canonical projection",
            );
            mark_source_baseline_ready(&mut baseline_ready);
            return;
        }
        cursor = initial_snapshot.cursor;
        let values = (
            initial_snapshot.revision,
            initial_snapshot.authorization_revision,
            initial_snapshot.redaction_revision.clone(),
            initial_snapshot.live.as_ref().map(|live| live.revision),
            initial_snapshot
                .live
                .as_ref()
                .is_some_and(|live| live.status.is_terminal()),
        );
        let baseline = envelope(
            &entry,
            revision,
            "execution",
            &source.id,
            source.detail_scope,
            Some(cursor),
            DeliveryClass::SnapshotReconstructable,
            SourceHealth::Baseline,
            "projection_snapshot",
            serde_json::to_value(initial_snapshot).unwrap_or_default(),
        );
        queue_envelope(
            &tx,
            &terminal,
            &entry,
            baseline,
            Some((source.key(), cursor, values.0)),
        );
        values
    };
    mark_source_baseline_ready(&mut baseline_ready);
    if !await_revision_release(&mut release).await || initial_terminal {
        return;
    }

    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    loop {
        interval.tick().await;
        let context = match runtime_routes::execution_projection_context(
            &state,
            &principal,
            &source.id,
            source.detail_scope,
        )
        .await
        {
            Ok(context) => context,
            Err((status, error))
                if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN =>
            {
                queue_source_revoke(
                    &tx,
                    &terminal,
                    &entry,
                    revision,
                    &source,
                    error.error.clone(),
                );
                return;
            }
            Err((_, error)) => {
                queue_source_resync(
                    &tx,
                    &terminal,
                    &entry,
                    revision,
                    &source,
                    cursor,
                    &error.error,
                );
                return;
            }
        };

        if let Some(live) = runtime.execution_live(&source.id) {
            if last_live_revision != Some(live.revision) {
                last_live_revision = Some(live.revision);
                let payload =
                    serde_json::to_value(harness_contract::projection::ExecutionLiveUpdate {
                        schema_version:
                            harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION,
                        execution_id: source.id.clone(),
                        live,
                    })
                    .unwrap_or_default();
                let update = envelope(
                    &entry,
                    revision,
                    "execution",
                    &source.id,
                    source.detail_scope,
                    None,
                    DeliveryClass::EphemeralPreview,
                    SourceHealth::Live,
                    "projection_live",
                    payload,
                );
                queue_envelope(&tx, &terminal, &entry, update, None);
            }
        }

        match runtime::execution_projection::delta(
            &runtime,
            &source.id,
            projection_revision,
            cursor,
            &context,
        ) {
            Ok(delta) if delta.resync_reason.is_some() => {
                queue_source_resync(
                    &tx,
                    &terminal,
                    &entry,
                    revision,
                    &source,
                    cursor,
                    &format!(
                        "execution projection delta requires resync: {:?}",
                        delta.resync_reason
                    ),
                );
                return;
            }
            Ok(delta)
                if delta.target_cursor > cursor
                    || delta.target_revision > projection_revision
                    || cursor == 0 =>
            {
                if delta.authorization_revision != projection_authorization_revision
                    || delta.redaction_revision != projection_redaction_revision
                    || delta.detail_scope != source.detail_scope
                {
                    queue_source_resync(
                        &tx,
                        &terminal,
                        &entry,
                        revision,
                        &source,
                        cursor,
                        "execution projection authority or redaction scope changed",
                    );
                    return;
                }
                let is_terminal = delta.operations.iter().any(|operation| {
                    matches!(
                        operation,
                        harness_contract::projection::ProjectionOperation::SetTerminal { .. }
                            | harness_contract::projection::ProjectionOperation::SetDeliveryTruth { .. }
                    )
                });
                cursor = delta.target_cursor;
                projection_revision = delta.target_revision;
                projection_authorization_revision = delta.authorization_revision;
                projection_redaction_revision = delta.redaction_revision.clone();
                let update = envelope(
                    &entry,
                    revision,
                    "execution",
                    &source.id,
                    source.detail_scope,
                    Some(cursor),
                    DeliveryClass::Durable,
                    SourceHealth::Live,
                    "projection_delta",
                    serde_json::to_value(delta).unwrap_or_default(),
                );
                queue_envelope(
                    &tx,
                    &terminal,
                    &entry,
                    update,
                    Some((source.key(), cursor, projection_revision)),
                );
                if is_terminal {
                    return;
                }
            }
            Ok(_) => {}
            Err(error) => {
                queue_source_resync(
                    &tx,
                    &terminal,
                    &entry,
                    revision,
                    &source,
                    cursor,
                    &error.to_string(),
                );
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_mission_source(
    state: Arc<AppState>,
    principal: AuthenticatedPrincipal,
    entry: Arc<SubscriptionEntry>,
    tx: mpsc::Sender<QueuedEnvelope>,
    terminal: Arc<Mutex<Option<TerminalSignal>>>,
    source: LiveSourceSelector,
    revision: u64,
    mut cursor: u64,
    baseline_ready: oneshot::Sender<()>,
    mut release: watch::Receiver<bool>,
) {
    let mut baseline_ready = Some(baseline_ready);
    if !mission_source_authorized(&state, &principal, &source.id) {
        queue_source_revoke(
            &tx,
            &terminal,
            &entry,
            revision,
            &source,
            "mission projection is outside the authenticated principal scope".to_string(),
        );
        mark_source_baseline_ready(&mut baseline_ready);
        return;
    }
    let mut commits = state.services.mission.subscribe_projection_commits();
    let initial_snapshot = match state.services.mission.materialized_snapshot().await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            queue_source_resync(&tx, &terminal, &entry, revision, &source, cursor, &error);
            mark_source_baseline_ready(&mut baseline_ready);
            return;
        }
    };
    cursor = initial_snapshot.cursor;
    let mut materialized_revision = initial_snapshot.revision;
    let baseline = envelope(
        &entry,
        revision,
        "mission",
        &source.id,
        source.detail_scope,
        Some(cursor),
        DeliveryClass::SnapshotReconstructable,
        SourceHealth::Baseline,
        "mission_snapshot",
        serde_json::to_value(initial_snapshot).unwrap_or_default(),
    );
    queue_envelope(
        &tx,
        &terminal,
        &entry,
        baseline,
        Some((source.key(), cursor, materialized_revision)),
    );
    mark_source_baseline_ready(&mut baseline_ready);
    if !await_revision_release(&mut release).await {
        return;
    }

    let mut authorization_check = tokio::time::interval_at(
        tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(30),
    );
    loop {
        let committed = tokio::select! {
            changed = commits.changed() => {
                if changed.is_err() {
                    return;
                }
                true
            }
            _ = authorization_check.tick() => false,
        };
        if !mission_source_authorized(&state, &principal, &source.id) {
            queue_source_revoke(
                &tx,
                &terminal,
                &entry,
                revision,
                &source,
                "mission projection is outside the authenticated principal scope".to_string(),
            );
            return;
        }
        if !committed {
            continue;
        }
        let delta = match state
            .services
            .mission
            .materialized_delta(cursor, Some(materialized_revision))
            .await
        {
            Ok(delta) => delta,
            Err(error) => {
                queue_source_resync(&tx, &terminal, &entry, revision, &source, cursor, &error);
                return;
            }
        };
        if delta.needs_resync {
            let snapshot = match state.services.mission.materialized_snapshot().await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    queue_source_resync(&tx, &terminal, &entry, revision, &source, cursor, &error);
                    return;
                }
            };
            cursor = snapshot.cursor;
            materialized_revision = snapshot.revision;
            let update = envelope(
                &entry,
                revision,
                "mission",
                &source.id,
                source.detail_scope,
                Some(cursor),
                DeliveryClass::SnapshotReconstructable,
                SourceHealth::Baseline,
                "mission_snapshot",
                serde_json::to_value(snapshot).unwrap_or_default(),
            );
            queue_envelope(
                &tx,
                &terminal,
                &entry,
                update,
                Some((source.key(), cursor, materialized_revision)),
            );
            continue;
        }
        if delta.to_cursor == cursor
            && delta.revision == materialized_revision
            && delta.changed_domains.is_empty()
        {
            continue;
        }
        cursor = delta.to_cursor;
        materialized_revision = delta.revision;
        let update = envelope(
            &entry,
            revision,
            "mission",
            &source.id,
            source.detail_scope,
            Some(cursor),
            DeliveryClass::Durable,
            SourceHealth::Live,
            "mission_delta",
            serde_json::to_value(delta).unwrap_or_default(),
        );
        queue_envelope(
            &tx,
            &terminal,
            &entry,
            update,
            Some((source.key(), cursor, materialized_revision)),
        );
    }
}

fn mark_source_baseline_ready(sender: &mut Option<oneshot::Sender<()>>) {
    if let Some(sender) = sender.take() {
        let _ = sender.send(());
    }
}

async fn await_revision_release(release: &mut watch::Receiver<bool>) -> bool {
    if *release.borrow() {
        return true;
    }
    release.changed().await.is_ok() && *release.borrow()
}

fn queue_session_payload(
    tx: &mpsc::Sender<QueuedEnvelope>,
    terminal: &Arc<Mutex<Option<TerminalSignal>>>,
    entry: &SubscriptionEntry,
    revision: u64,
    source: &LiveSourceSelector,
    payload: serde_json::Value,
    cursor: Option<u64>,
) {
    let event_name = payload
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("session.event");
    let delivery = match event_name {
        "UserMessageCommitted" | "TerminalCommitted" => DeliveryClass::Durable,
        "TextDelta" => DeliveryClass::EphemeralPreview,
        "TerminalDelivery" => match payload
            .get("delivery")
            .and_then(|delivery| delivery.get("event"))
            .and_then(serde_json::Value::as_str)
        {
            Some("text_delta") => DeliveryClass::EphemeralPreview,
            Some("terminal_presentation_committed" | "cancellation_committed") => {
                DeliveryClass::Durable
            }
            _ => DeliveryClass::SnapshotReconstructable,
        },
        _ => DeliveryClass::SnapshotReconstructable,
    };
    let health = if matches!(event_name, "session_stream_resync" | "RuntimeStreamLagged") {
        SourceHealth::ResyncRequired
    } else {
        SourceHealth::Live
    };
    let mut result = envelope(
        entry,
        revision,
        "session",
        &source.id,
        source.detail_scope,
        cursor,
        delivery,
        health,
        event_name,
        payload.clone(),
    );
    result.execution_id = string_field(&payload, "execution_id");
    result.mission_id = string_field(&payload, "mission_id");
    result.agent_id = string_field(&payload, "agent_id");
    let typed_text_delta = payload.get("delivery").filter(|delivery| {
        delivery.get("event").and_then(serde_json::Value::as_str) == Some("text_delta")
    });
    result.stream_revision = typed_text_delta
        .and_then(|delivery| u64_field(delivery, "byte_end"))
        .or_else(|| u64_field(&payload, "stream_revision"));
    result.start_bytes = typed_text_delta
        .and_then(|delivery| u64_field(delivery, "byte_start"))
        .or_else(|| u64_field(&payload, "start_bytes"));
    result.end_bytes = typed_text_delta
        .and_then(|delivery| u64_field(delivery, "byte_end"))
        .or_else(|| u64_field(&payload, "end_bytes"));
    queue_envelope(
        tx,
        terminal,
        entry,
        result,
        cursor.map(|value| (source.key(), value, source.revision)),
    );
}

fn queue_source_revoke(
    tx: &mpsc::Sender<QueuedEnvelope>,
    terminal: &Arc<Mutex<Option<TerminalSignal>>>,
    entry: &SubscriptionEntry,
    revision: u64,
    source: &LiveSourceSelector,
    reason: String,
) {
    let (kind, id) = source_parts(source);
    let revoked = envelope(
        entry,
        revision,
        kind,
        id,
        source.detail_scope,
        None,
        DeliveryClass::SnapshotReconstructable,
        SourceHealth::Revoked,
        "source.authorization_revoked",
        serde_json::json!({"reason": reason}),
    );
    queue_envelope(tx, terminal, entry, revoked, None);
}

#[allow(clippy::too_many_arguments)]
fn queue_source_resync(
    tx: &mpsc::Sender<QueuedEnvelope>,
    terminal: &Arc<Mutex<Option<TerminalSignal>>>,
    entry: &SubscriptionEntry,
    revision: u64,
    source: &LiveSourceSelector,
    cursor: u64,
    reason: &str,
) {
    let (kind, id) = source_parts(source);
    let resync = envelope(
        entry,
        revision,
        kind,
        id,
        source.detail_scope,
        Some(cursor),
        DeliveryClass::SnapshotReconstructable,
        SourceHealth::ResyncRequired,
        "source.resync_required",
        serde_json::json!({"reason": reason, "cursor": cursor}),
    );
    queue_envelope(
        tx,
        terminal,
        entry,
        resync,
        Some((source.key(), cursor, source.revision)),
    );
}

fn queue_envelope(
    tx: &mpsc::Sender<QueuedEnvelope>,
    terminal: &Arc<Mutex<Option<TerminalSignal>>>,
    entry: &SubscriptionEntry,
    envelope: LiveEnvelope,
    checkpoint_update: Option<(String, u64, u64)>,
) {
    let delivery = envelope.delivery_class;
    match tx.try_send(QueuedEnvelope {
        envelope,
        checkpoint_update,
    }) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(queued))
            if delivery == DeliveryClass::EphemeralPreview =>
        {
            let preview_key = pending_preview_key(&queued.envelope);
            let mut pending = entry
                .pending_previews
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let accepted = match pending.entry(preview_key) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(queued);
                    true
                }
                std::collections::btree_map::Entry::Occupied(mut slot)
                    if is_text_preview_delta(&slot.get().envelope)
                        && is_text_preview_delta(&queued.envelope) =>
                {
                    let merged = merge_text_delta(slot.get_mut(), queued).is_ok();
                    if !merged {
                        slot.remove();
                    }
                    merged
                }
                std::collections::btree_map::Entry::Occupied(slot) => {
                    // A key collision that cannot be proven contiguous must
                    // never silently replace visible bytes. Force the Surface
                    // to recover from its durable projection instead.
                    slot.remove();
                    false
                }
            };
            drop(pending);
            if !accepted {
                signal_terminal(
                    tx,
                    terminal,
                    "subscription.resync_required",
                    "non-contiguous assistant preview reached the bounded Surface queue",
                );
                return;
            }
            runtime::execution_core::performance::observe_bytes(
                "surface_preview_coalesced_total",
                1,
            );
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            signal_terminal(
                tx,
                terminal,
                "subscription.resync_required",
                "bounded Surface queue reached its recovery boundary",
            );
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

fn pending_preview_key(envelope: &LiveEnvelope) -> String {
    let payload_field = |name| {
        envelope
            .payload
            .get(name)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
    };
    let delivery_field = |name| {
        envelope
            .payload
            .get("delivery")
            .and_then(|delivery| delivery.get(name))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
    };
    format!(
        "{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
        envelope.source_kind,
        envelope.source_id,
        envelope.event,
        envelope.execution_id.as_deref().unwrap_or_default(),
        payload_field("turn_id"),
        payload_field("part_id"),
        delivery_field("presentation_id"),
        delivery_field("attempt_id"),
    )
}

fn is_text_preview_delta(envelope: &LiveEnvelope) -> bool {
    envelope.event == "TextDelta" || terminal_delivery_text_delta(&envelope.payload).is_some()
}

fn terminal_delivery_text_delta(
    payload: &serde_json::Value,
) -> Option<(&str, &str, u64, u64, &str)> {
    let delivery = payload.get("delivery")?;
    (delivery.get("event")?.as_str()? == "text_delta").then_some((
        delivery.get("presentation_id")?.as_str()?,
        delivery.get("attempt_id")?.as_str()?,
        delivery.get("byte_start")?.as_u64()?,
        delivery.get("byte_end")?.as_u64()?,
        delivery.get("delta")?.as_str()?,
    ))
}

fn merge_text_delta(existing: &mut QueuedEnvelope, mut incoming: QueuedEnvelope) -> Result<(), ()> {
    if existing.envelope.event == "TerminalDelivery"
        || incoming.envelope.event == "TerminalDelivery"
    {
        let (existing_presentation, existing_attempt, existing_start, existing_end, existing_text) =
            terminal_delivery_text_delta(&existing.envelope.payload).ok_or(())?;
        let (incoming_presentation, incoming_attempt, incoming_start, incoming_end, incoming_text) =
            terminal_delivery_text_delta(&incoming.envelope.payload).ok_or(())?;
        if existing_presentation != incoming_presentation
            || existing_attempt != incoming_attempt
            || existing_end != incoming_start
            || incoming_end < incoming_start
        {
            return Err(());
        }
        let mut text =
            String::with_capacity(existing_text.len().saturating_add(incoming_text.len()));
        text.push_str(existing_text);
        text.push_str(incoming_text);
        if text.len() > MAX_PENDING_TEXT_PREVIEW_BYTES
            || u64::try_from(text.len()).ok() != Some(incoming_end.saturating_sub(existing_start))
        {
            return Err(());
        }
        let delivery = incoming
            .envelope
            .payload
            .get_mut("delivery")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or(())?;
        delivery.insert("delta".to_string(), serde_json::Value::String(text));
        delivery.insert("byte_start".to_string(), existing_start.into());
        delivery.insert("byte_end".to_string(), incoming_end.into());
        incoming.envelope.start_bytes = Some(existing_start);
        incoming.envelope.end_bytes = Some(incoming_end);
        incoming.envelope.stream_revision = Some(incoming_end);
        *existing = incoming;
        return Ok(());
    }
    let existing_start = existing.envelope.start_bytes.ok_or(())?;
    let existing_end = existing.envelope.end_bytes.ok_or(())?;
    let incoming_start = incoming.envelope.start_bytes.ok_or(())?;
    let incoming_end = incoming.envelope.end_bytes.ok_or(())?;
    if existing_end != incoming_start || incoming_end < incoming_start {
        return Err(());
    }
    let existing_text = existing
        .envelope
        .payload
        .get("text")
        .and_then(serde_json::Value::as_str)
        .ok_or(())?;
    let incoming_text = incoming
        .envelope
        .payload
        .get("text")
        .and_then(serde_json::Value::as_str)
        .ok_or(())?;
    let mut text = String::with_capacity(existing_text.len().saturating_add(incoming_text.len()));
    text.push_str(existing_text);
    text.push_str(incoming_text);
    if text.len() > MAX_PENDING_TEXT_PREVIEW_BYTES {
        return Err(());
    }
    if u64::try_from(text.len()).ok() != Some(incoming_end.saturating_sub(existing_start)) {
        return Err(());
    }
    let payload = incoming.envelope.payload.as_object_mut().ok_or(())?;
    payload.insert("text".to_string(), serde_json::Value::String(text));
    payload.insert("start_bytes".to_string(), existing_start.into());
    payload.insert("end_bytes".to_string(), incoming_end.into());
    payload.insert("stream_revision".to_string(), incoming_end.into());
    incoming.envelope.start_bytes = Some(existing_start);
    incoming.envelope.end_bytes = Some(incoming_end);
    incoming.envelope.stream_revision = Some(incoming_end);
    *existing = incoming;
    Ok(())
}

fn signal_terminal(
    tx: &mpsc::Sender<QueuedEnvelope>,
    terminal: &Arc<Mutex<Option<TerminalSignal>>>,
    event: &str,
    reason: &str,
) {
    let mut slot = terminal
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if slot.is_none() {
        *slot = Some(TerminalSignal {
            event: event.to_string(),
            reason: reason.to_string(),
        });
        let _ = tx.try_send(QueuedEnvelope::wake());
    }
}

#[allow(clippy::too_many_arguments)]
fn envelope(
    entry: &SubscriptionEntry,
    revision: u64,
    source_kind: &str,
    source_id: &str,
    detail_scope: harness_contract::projection::ProjectionDetailScope,
    source_cursor: Option<u64>,
    delivery_class: DeliveryClass,
    source_health: SourceHealth,
    event: &str,
    payload: serde_json::Value,
) -> LiveEnvelope {
    LiveEnvelope {
        schema_version: LIVE_CONTRACT_SCHEMA_VERSION,
        subscription_id: entry.id.clone(),
        subscription_revision: revision,
        source_kind: source_kind.to_string(),
        source_id: source_id.to_string(),
        detail_scope,
        source_cursor,
        delivery_class,
        source_health,
        event: event.to_string(),
        payload,
        session_id: (source_kind == "session").then(|| source_id.to_string()),
        execution_id: (source_kind == "execution").then(|| source_id.to_string()),
        mission_id: (source_kind == "mission").then(|| source_id.to_string()),
        agent_id: None,
        stream_revision: None,
        start_bytes: None,
        end_bytes: None,
    }
}

struct QueuedEnvelope {
    envelope: LiveEnvelope,
    checkpoint_update: Option<(String, u64, u64)>,
}

impl QueuedEnvelope {
    fn wake() -> Self {
        Self {
            envelope: LiveEnvelope {
                schema_version: LIVE_CONTRACT_SCHEMA_VERSION,
                subscription_id: String::new(),
                subscription_revision: 0,
                source_kind: "subscription".to_string(),
                source_id: String::new(),
                detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
                source_cursor: None,
                delivery_class: DeliveryClass::EphemeralPreview,
                source_health: SourceHealth::Live,
                event: "subscription.wake".to_string(),
                payload: serde_json::Value::Null,
                session_id: None,
                execution_id: None,
                mission_id: None,
                agent_id: None,
                stream_revision: None,
                start_bytes: None,
                end_bytes: None,
            },
            checkpoint_update: None,
        }
    }
}

struct TerminalSignal {
    event: String,
    reason: String,
}

struct PhysicalLiveStream {
    rx: ReceiverStream<QueuedEnvelope>,
    entry: Arc<SubscriptionEntry>,
    terminal: Arc<Mutex<Option<TerminalSignal>>>,
    delivered_cursors: Arc<Mutex<BTreeMap<String, u64>>>,
    delivered_revisions: Arc<Mutex<BTreeMap<String, u64>>>,
    checkpoint_secret: [u8; 32],
    ready_revision: u64,
    ended: bool,
}

impl Stream for PhysicalLiveStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        let terminal = self
            .terminal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(terminal) = terminal {
            let snapshot = self.entry.snapshot();
            let envelope = envelope(
                &self.entry,
                snapshot.revision,
                "subscription",
                &self.entry.id,
                harness_contract::projection::ProjectionDetailScope::Summary,
                None,
                DeliveryClass::SnapshotReconstructable,
                SourceHealth::ResyncRequired,
                &terminal.event,
                serde_json::json!({"reason": terminal.reason}),
            );
            self.ended = true;
            let mut event = Event::default()
                .event("live")
                .json_data(envelope)
                .unwrap_or_default();
            if let Ok(checkpoint) = self.checkpoint_token(&snapshot) {
                event = event.id(checkpoint);
            }
            return Poll::Ready(Some(Ok(event)));
        }

        loop {
            let (queued, channel_closed) = match self.rx.poll_next_unpin(cx) {
                Poll::Ready(Some(queued)) => (Some(queued), false),
                Poll::Ready(None) => (self.take_pending_preview(), true),
                Poll::Pending => (self.take_pending_preview(), false),
            };
            match queued {
                Some(queued) => {
                    let snapshot = self.entry.snapshot();
                    if queued.envelope.subscription_revision != snapshot.revision
                        && queued.envelope.subscription_revision != self.ready_revision
                    {
                        continue;
                    }
                    let revision_barrier = matches!(
                        queued.envelope.event.as_str(),
                        "subscription.ready" | "subscription.revision.changed"
                    );
                    if revision_barrier
                        && queued.envelope.subscription_revision == snapshot.revision
                    {
                        self.ready_revision = snapshot.revision;
                    }
                    let writes_checkpoint = queued.checkpoint_update.is_some() || revision_barrier;
                    if let Some((source, cursor, revision)) = queued.checkpoint_update {
                        let source_revision_key = source.clone();
                        let mut delivered = self
                            .delivered_cursors
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        let current = delivered.entry(source).or_default();
                        *current = (*current).max(cursor);
                        let mut delivered_revisions = self
                            .delivered_revisions
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        let current_revision =
                            delivered_revisions.entry(source_revision_key).or_default();
                        *current_revision = (*current_revision).max(revision);
                    }
                    let event = Event::default()
                        .event("live")
                        .json_data(queued.envelope)
                        .unwrap_or_default();
                    if !writes_checkpoint {
                        return Poll::Ready(Some(Ok(event)));
                    }
                    let checkpoint = match self.checkpoint_token(&snapshot) {
                        Ok(checkpoint) => checkpoint,
                        Err(reason) => {
                            let envelope = envelope(
                                &self.entry,
                                snapshot.revision,
                                "subscription",
                                &self.entry.id,
                                harness_contract::projection::ProjectionDetailScope::Summary,
                                None,
                                DeliveryClass::SnapshotReconstructable,
                                SourceHealth::ResyncRequired,
                                "checkpoint_overflow",
                                serde_json::json!({"reason": reason}),
                            );
                            self.ended = true;
                            return Poll::Ready(Some(Ok(Event::default()
                                .event("live")
                                .json_data(envelope)
                                .unwrap_or_default())));
                        }
                    };
                    let event = event.id(checkpoint);
                    return Poll::Ready(Some(Ok(event)));
                }
                None if channel_closed => {
                    self.ended = true;
                    return Poll::Ready(None);
                }
                None => return Poll::Pending,
            }
        }
    }
}

impl PhysicalLiveStream {
    fn take_pending_preview(&self) -> Option<QueuedEnvelope> {
        self.entry
            .pending_previews
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop_first()
            .map(|(_, queued)| queued)
    }

    fn checkpoint_token(&self, snapshot: &SubscriptionRevision) -> Result<String, String> {
        let key_revision = checkpoint_key_revision(now_ms());
        let checkpoint = CompositeCheckpoint {
            schema_version: LIVE_CONTRACT_SCHEMA_VERSION,
            subscription_id: self.entry.id.clone(),
            subscription_revision: snapshot.revision,
            selector_hash: snapshot.selector_hash.clone(),
            principal_hash: self.entry.principal_hash.clone(),
            surface_instance_hash: self.entry.surface_instance_hash.clone(),
            issued_at_ms: now_ms(),
            expires_at_ms: snapshot.expires_at_ms,
            key_revision,
            source_cursors: self
                .delivered_cursors
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
            source_revisions: self
                .delivered_revisions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
        };
        let key = derive_checkpoint_key(&self.checkpoint_secret, key_revision)?;
        sign_checkpoint(&checkpoint, &key, self.entry.limits.checkpoint_max_bytes)
    }
}

impl Drop for PhysicalLiveStream {
    fn drop(&mut self) {
        self.entry
            .pending_previews
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        self.entry.active_connection.store(false, Ordering::Release);
    }
}

fn normalize_selector(
    selector: LiveSelector,
    max_sources: usize,
) -> Result<LiveSelector, (StatusCode, Json<ErrorResponse>)> {
    if selector.sources.len() > max_sources {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("live selector supports at most {max_sources} sources"),
        ));
    }
    let mut sources = selector.sources;
    for source in &mut sources {
        source.id = source.id.trim().to_string();
        if source.id.is_empty() || source.id.len() > MAX_SOURCE_ID_BYTES {
            return Err(api_error(
                StatusCode::BAD_REQUEST,
                "live source id is empty or too long",
            ));
        }
    }
    sources.sort_by_key(LiveSourceSelector::key);
    if sources
        .windows(2)
        .any(|pair| pair[0].key() == pair[1].key())
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "live selector contains duplicate sources",
        ));
    }
    Ok(LiveSelector { sources })
}

async fn validate_selector_authority(
    state: &AppState,
    principal: &AuthenticatedPrincipal,
    selector: &LiveSelector,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    for source in &selector.sources {
        match source.kind {
            LiveSourceKind::Session => {
                authorize_session_access(state, principal, &source.id, SessionAccess::Read).await?;
            }
            LiveSourceKind::Execution => {
                runtime_routes::execution_projection_context(
                    state,
                    principal,
                    &source.id,
                    source.detail_scope,
                )
                .await?;
            }
            LiveSourceKind::Mission => {
                if !mission_source_authorized(state, principal, &source.id) {
                    return Err(api_error(
                        StatusCode::FORBIDDEN,
                        "mission projection is outside the authenticated principal scope",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn mission_source_authorized(
    state: &AppState,
    principal: &AuthenticatedPrincipal,
    mission_id: &str,
) -> bool {
    let projection = state.services.mission.projection();
    let canonical_id = projection
        .pointer("/mission/mission_id")
        .and_then(serde_json::Value::as_str);
    if canonical_id != Some(mission_id) {
        return false;
    }
    let claims = principal.0.claims();
    claims
        .scopes
        .iter()
        .any(|scope| scope == &format!("mission:{mission_id}"))
        || (principal.0.is_human_interactive() && principal.0.has_capability("mission.observe"))
}

fn validate_surface_instance(value: &str) -> Result<String, (StatusCode, Json<ErrorResponse>)> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > MAX_SURFACE_INSTANCE_BYTES
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ':' | '-' | '_' | '.'))
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "surface_instance is invalid",
        ));
    }
    Ok(value.to_string())
}

fn require_surface_instance(
    headers: &HeaderMap,
    fallback: Option<&str>,
) -> Result<String, (StatusCode, Json<ErrorResponse>)> {
    let header = headers
        .get(SURFACE_INSTANCE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match (header, fallback) {
        (Some(header), Some(fallback)) if header != fallback => Err(api_error(
            StatusCode::FORBIDDEN,
            "Surface instance header and request binding do not match",
        )),
        (Some(header), _) => validate_surface_instance(header),
        (None, Some(fallback)) => validate_surface_instance(fallback),
        (None, None) => Err(api_error(
            StatusCode::BAD_REQUEST,
            "Surface instance binding is required",
        )),
    }
}

fn validate_idempotency_key(
    value: Option<&str>,
) -> Result<Option<String>, (StatusCode, Json<ErrorResponse>)> {
    let value = value.map(str::trim).filter(|value| !value.is_empty());
    if value.is_some_and(|value| value.len() > MAX_IDEMPOTENCY_KEY_BYTES) {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "idempotency_key is too long",
        ));
    }
    Ok(value.map(str::to_string))
}

fn validate_required_idempotency_key(
    value: &str,
) -> Result<String, (StatusCode, Json<ErrorResponse>)> {
    validate_idempotency_key(Some(value))?.ok_or_else(|| {
        api_error(
            StatusCode::BAD_REQUEST,
            "idempotency_key is required for live subscription PATCH",
        )
    })
}

fn patch_request_hash(
    expected_revision: u64,
    selector: &LiveSelector,
    ttl_seconds: Option<u64>,
) -> String {
    let payload = serde_json::json!({
        "expected_revision": expected_revision,
        "selector": selector,
        "ttl_seconds": ttl_seconds,
    });
    hex_hash(&serde_json::to_vec(&payload).unwrap_or_default())
}

fn create_request_hash(
    surface_instance: &str,
    selector: &LiveSelector,
    ttl_seconds: u64,
) -> String {
    let payload = serde_json::json!({
        "surface_instance": surface_instance,
        "selector": selector,
        "ttl_seconds": ttl_seconds,
    });
    hex_hash(&serde_json::to_vec(&payload).unwrap_or_default())
}

fn expiry_from_ttl(
    ttl_seconds: Option<u64>,
    limits: &runtime::GatewayLiveConfig,
) -> Result<u64, (StatusCode, Json<ErrorResponse>)> {
    let ttl = ttl_seconds.unwrap_or(limits.default_ttl_seconds);
    if ttl == 0 || ttl > limits.max_ttl_seconds {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("ttl_seconds must be in 1..={}", limits.max_ttl_seconds),
        ));
    }
    Ok(now_ms().saturating_add(ttl.saturating_mul(1_000)))
}

fn selector_hash(selector: &LiveSelector) -> String {
    let encoded = serde_json::to_vec(selector).unwrap_or_default();
    hex_hash(&encoded)
}

fn principal_binding(principal: &AuthenticatedPrincipal) -> String {
    let claims = principal.0.claims();
    format!(
        "{}:{}:{}",
        claims.principal_id, claims.credential_epoch, claims.profile_revision
    )
}

fn hash_text(value: &str) -> String {
    hex_hash(value.as_bytes())
}

fn hex_hash(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sign_checkpoint(
    checkpoint: &CompositeCheckpoint,
    secret: &[u8; 32],
    max_bytes: usize,
) -> Result<String, String> {
    let payload = serde_json::to_vec(checkpoint)
        .map_err(|error| format!("checkpoint serialization failed: {error}"))?;
    let encoded = URL_SAFE_NO_PAD.encode(payload);
    let mut mac =
        HmacSha256::new_from_slice(secret).map_err(|_| "checkpoint key is invalid".to_string())?;
    mac.update(encoded.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    let token = format!("{encoded}.{signature}");
    if token.len() > max_bytes {
        return Err("composite checkpoint exceeds the transport header limit".to_string());
    }
    Ok(token)
}

fn checkpoint_key_revision(timestamp_ms: u64) -> u64 {
    timestamp_ms / CHECKPOINT_KEY_ROTATION_MS
}

fn derive_checkpoint_key(master: &[u8; 32], revision: u64) -> Result<[u8; 32], String> {
    let mut mac =
        HmacSha256::new_from_slice(master).map_err(|_| "checkpoint key is invalid".to_string())?;
    mac.update(b"cowd-live-checkpoint-key");
    mac.update(&revision.to_le_bytes());
    Ok(mac.finalize().into_bytes().into())
}

fn verify_checkpoint(
    token: &str,
    entry: &SubscriptionEntry,
    revision: &SubscriptionRevision,
    checkpoint_secret: &[u8; 32],
) -> Result<CompositeCheckpoint, String> {
    if token.len() > entry.limits.checkpoint_max_bytes {
        return Err("composite checkpoint exceeds the transport header limit".to_string());
    }
    let (encoded, signature) = token
        .split_once('.')
        .ok_or_else(|| "composite checkpoint is malformed".to_string())?;
    let payload = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| "composite checkpoint payload is malformed".to_string())?;
    let checkpoint: CompositeCheckpoint = serde_json::from_slice(&payload)
        .map_err(|_| "composite checkpoint payload is invalid".to_string())?;
    let current_key_revision = checkpoint_key_revision(now_ms());
    let retained_revisions = checkpoint_key_retention_revisions(entry.limits.max_ttl_seconds);
    if checkpoint.key_revision > current_key_revision
        || current_key_revision.saturating_sub(checkpoint.key_revision) > retained_revisions
    {
        return Err("composite checkpoint signing key is no longer valid".to_string());
    }
    let signature = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| "composite checkpoint signature is malformed".to_string())?;
    let key = derive_checkpoint_key(checkpoint_secret, checkpoint.key_revision)?;
    let mut mac =
        HmacSha256::new_from_slice(&key).map_err(|_| "checkpoint key is invalid".to_string())?;
    mac.update(encoded.as_bytes());
    mac.verify_slice(&signature)
        .map_err(|_| "composite checkpoint signature is invalid".to_string())?;
    if checkpoint.schema_version != LIVE_CONTRACT_SCHEMA_VERSION
        || checkpoint.subscription_id != entry.id
        || checkpoint.subscription_revision != revision.revision
        || checkpoint.selector_hash != revision.selector_hash
        || checkpoint.principal_hash != entry.principal_hash
        || checkpoint.surface_instance_hash != entry.surface_instance_hash
        || checkpoint.issued_at_ms > now_ms().saturating_add(30_000)
        || checkpoint.issued_at_ms >= checkpoint.expires_at_ms
        || checkpoint.expires_at_ms <= now_ms()
    {
        return Err("composite checkpoint binding is stale or mismatched".to_string());
    }
    let allowed = revision
        .selector
        .sources
        .iter()
        .map(LiveSourceSelector::key)
        .collect::<BTreeSet<_>>();
    if checkpoint
        .source_cursors
        .keys()
        .chain(checkpoint.source_revisions.keys())
        .any(|source| !allowed.contains(source))
    {
        return Err("composite checkpoint contains an unselected source".to_string());
    }
    Ok(checkpoint)
}

fn source_parts(source: &LiveSourceSelector) -> (&'static str, &str) {
    match source.kind {
        LiveSourceKind::Session => ("session", &source.id),
        LiveSourceKind::Execution => ("execution", &source.id),
        LiveSourceKind::Mission => ("mission", &source.id),
    }
}

fn split_source_key(key: &str) -> (&str, &str) {
    key.split_once(':').unwrap_or(("source", key))
}

fn string_field(value: &serde_json::Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn u64_field(value: &serde_json::Value, field: &str) -> Option<u64> {
    value.get(field).and_then(serde_json::Value::as_u64)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

fn checkpoint_key_retention_revisions(max_ttl_seconds: u64) -> u64 {
    (max_ttl_seconds.saturating_mul(1_000) / CHECKPOINT_KEY_ROTATION_MS).saturating_add(1)
}

fn gateway_live_config(state: &AppState) -> runtime::GatewayLiveConfig {
    let defaults = runtime::GatewayLiveConfig::default();
    let Some(value) = state
        .config
        .as_ref()
        .and_then(|config| config.get("gateway"))
        .and_then(|gateway| gateway.get("live"))
    else {
        return defaults;
    };
    let usize_value = |name: &str, fallback: usize| {
        value
            .get(name)
            .and_then(serde_json::Value::as_u64)
            .and_then(|number| usize::try_from(number).ok())
            .unwrap_or(fallback)
    };
    let u64_value = |name: &str, fallback: u64| {
        value
            .get(name)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(fallback)
    };
    let parsed = runtime::GatewayLiveConfig {
        max_sources: usize_value("max_sources", defaults.max_sources),
        max_subscriptions_per_principal_instance: usize_value(
            "max_subscriptions_per_principal_instance",
            defaults.max_subscriptions_per_principal_instance,
        ),
        queue_capacity: usize_value("queue_capacity", defaults.queue_capacity),
        checkpoint_max_bytes: usize_value("checkpoint_max_bytes", defaults.checkpoint_max_bytes),
        default_ttl_seconds: u64_value("default_ttl_seconds", defaults.default_ttl_seconds),
        max_ttl_seconds: u64_value("max_ttl_seconds", defaults.max_ttl_seconds),
        baseline_timeout_ms: u64_value("baseline_timeout_ms", defaults.baseline_timeout_ms),
    };
    if parsed.max_sources == 0
        || parsed.max_subscriptions_per_principal_instance == 0
        || parsed.queue_capacity == 0
        || parsed.checkpoint_max_bytes < 1_024
        || parsed.default_ttl_seconds == 0
        || parsed.max_ttl_seconds < parsed.default_ttl_seconds
        || parsed.baseline_timeout_ms == 0
    {
        defaults
    } else {
        parsed
    }
}

fn api_error(status: StatusCode, message: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            error: message.into(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::projection::ProjectionDetailScope;

    fn test_source(id: &str) -> LiveSourceSelector {
        LiveSourceSelector {
            kind: LiveSourceKind::Session,
            id: id.to_string(),
            cursor: 0,
            revision: 0,
            detail_scope: ProjectionDetailScope::Summary,
        }
    }

    fn test_entry(selector: LiveSelector) -> Arc<SubscriptionEntry> {
        let selector_hash = selector_hash(&selector);
        let revision = Arc::new(SubscriptionRevision {
            revision: 1,
            selector,
            selector_hash,
            expires_at_ms: now_ms() + 60_000,
            deleted: false,
        });
        let (revisions, _) = watch::channel(revision);
        Arc::new(SubscriptionEntry {
            id: "subscription".to_string(),
            principal_binding: "principal:1:1".to_string(),
            principal_hash: hash_text("principal:1:1"),
            surface_instance: "webui:test".to_string(),
            surface_instance_hash: hash_text("webui:test"),
            idempotency_key: Some("create-1".to_string()),
            create_request_hash: "create-hash".to_string(),
            limits: runtime::GatewayLiveConfig::default(),
            patch_lock: AsyncMutex::new(()),
            patch_idempotency: Mutex::new(HashMap::new()),
            pending_previews: Mutex::new(BTreeMap::new()),
            revisions,
            active_connection: AtomicBool::new(false),
        })
    }

    fn queued_text_delta(start: u64, text: &str) -> QueuedEnvelope {
        let end = start.saturating_add(u64::try_from(text.len()).unwrap());
        let mut envelope = envelope(
            &test_entry(LiveSelector {
                sources: vec![test_source("session-a")],
            }),
            1,
            "session",
            "session-a",
            ProjectionDetailScope::Summary,
            Some(end),
            DeliveryClass::EphemeralPreview,
            SourceHealth::Live,
            "TextDelta",
            serde_json::json!({
                "type": "TextDelta",
                "execution_id": "execution-a",
                "turn_id": "turn-a",
                "part_id": "item-text-1:text:0",
                "text": text,
                "start_bytes": start,
                "end_bytes": end,
                "stream_revision": end,
            }),
        );
        envelope.execution_id = Some("execution-a".to_string());
        envelope.start_bytes = Some(start);
        envelope.end_bytes = Some(end);
        envelope.stream_revision = Some(end);
        QueuedEnvelope {
            envelope,
            checkpoint_update: Some(("session:session-a".to_string(), end, 0)),
        }
    }

    fn queued_terminal_delivery_delta(
        presentation_id: &str,
        attempt_id: &str,
        start: u64,
        text: &str,
    ) -> QueuedEnvelope {
        let end = start.saturating_add(u64::try_from(text.len()).unwrap());
        let mut envelope = envelope(
            &test_entry(LiveSelector {
                sources: vec![test_source("session-a")],
            }),
            1,
            "session",
            "session-a",
            ProjectionDetailScope::Summary,
            Some(end),
            DeliveryClass::EphemeralPreview,
            SourceHealth::Live,
            "TerminalDelivery",
            serde_json::json!({
                "type": "TerminalDelivery",
                "execution_id": "execution-a",
                "turn_id": "turn-a",
                "delivery": {
                    "event": "text_delta",
                    "presentation_id": presentation_id,
                    "attempt_id": attempt_id,
                    "byte_start": start,
                    "byte_end": end,
                    "delta": text,
                },
            }),
        );
        envelope.execution_id = Some("execution-a".to_string());
        envelope.start_bytes = Some(start);
        envelope.end_bytes = Some(end);
        envelope.stream_revision = Some(end);
        QueuedEnvelope {
            envelope,
            checkpoint_update: Some(("session:session-a".to_string(), end, 0)),
        }
    }

    #[test]
    fn pending_text_deltas_merge_only_when_byte_ranges_are_contiguous() {
        let mut first = queued_text_delta(0, "第一段");
        let first_end = first.envelope.end_bytes.unwrap();
        merge_text_delta(&mut first, queued_text_delta(first_end, "second")).unwrap();
        assert_eq!(first.envelope.start_bytes, Some(0));
        assert_eq!(
            first.envelope.payload["text"],
            serde_json::Value::String("第一段second".to_string())
        );
        assert_eq!(
            first.envelope.end_bytes,
            Some(u64::try_from("第一段second".len()).unwrap())
        );

        let mut gap = queued_text_delta(0, "a");
        assert!(merge_text_delta(&mut gap, queued_text_delta(2, "b")).is_err());
    }

    #[test]
    fn pending_text_delta_coalesces_one_thousand_utf8_ranges_without_loss() {
        let fragments = (0..1_000)
            .map(|index| format!("第{index}段"))
            .collect::<Vec<_>>();
        let expected = fragments.concat();
        let mut merged = queued_text_delta(0, &fragments[0]);

        for fragment in fragments.iter().skip(1) {
            let start = merged.envelope.end_bytes.expect("merged byte cursor");
            merge_text_delta(&mut merged, queued_text_delta(start, fragment))
                .expect("contiguous UTF-8 range must merge");
        }

        assert_eq!(merged.envelope.start_bytes, Some(0));
        assert_eq!(merged.envelope.end_bytes, Some(expected.len() as u64));
        assert_eq!(merged.envelope.stream_revision, Some(expected.len() as u64));
        assert_eq!(merged.envelope.payload["text"], expected);
    }

    #[test]
    fn saturated_typed_preview_preserves_attempt_identity_and_contiguous_bytes() {
        let entry = test_entry(LiveSelector {
            sources: vec![test_source("session-a")],
        });
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(QueuedEnvelope::wake()).expect("saturate queue");
        let terminal = Arc::new(Mutex::new(None));

        for queued in [
            queued_terminal_delivery_delta("presentation-a", "attempt-1", 0, "第一段"),
            queued_terminal_delivery_delta(
                "presentation-a",
                "attempt-1",
                u64::try_from("第一段".len()).unwrap(),
                "second",
            ),
            queued_terminal_delivery_delta("presentation-a", "attempt-2", 0, "retry"),
            queued_terminal_delivery_delta("presentation-b", "attempt-1", 0, "other"),
        ] {
            queue_envelope(
                &tx,
                &terminal,
                &entry,
                queued.envelope,
                queued.checkpoint_update,
            );
        }

        let pending = entry.pending_previews.lock().unwrap();
        assert_eq!(
            pending.len(),
            3,
            "presentation and attempt identities must never overwrite each other"
        );
        let merged = pending
            .get(&pending_preview_key(
                &queued_terminal_delivery_delta("presentation-a", "attempt-1", 0, "").envelope,
            ))
            .expect("merged first attempt");
        assert_eq!(merged.envelope.payload["delivery"]["delta"], "第一段second");
        assert_eq!(merged.envelope.start_bytes, Some(0));
        assert_eq!(
            merged.envelope.end_bytes,
            Some(u64::try_from("第一段second".len()).unwrap())
        );
        assert!(terminal.lock().unwrap().is_none());
    }

    #[test]
    fn saturated_typed_preview_gap_forces_resync_instead_of_silent_overwrite() {
        let entry = test_entry(LiveSelector {
            sources: vec![test_source("session-a")],
        });
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(QueuedEnvelope::wake()).expect("saturate queue");
        let terminal = Arc::new(Mutex::new(None));

        for queued in [
            queued_terminal_delivery_delta("presentation-a", "attempt-1", 0, "a"),
            queued_terminal_delivery_delta("presentation-a", "attempt-1", 2, "b"),
        ] {
            queue_envelope(
                &tx,
                &terminal,
                &entry,
                queued.envelope,
                queued.checkpoint_update,
            );
        }

        assert!(
            entry.pending_previews.lock().unwrap().is_empty(),
            "a discontinuous preview must not leave either byte range as an apparently valid preview"
        );
        let terminal = terminal.lock().unwrap();
        let terminal = terminal.as_ref().expect("explicit recovery boundary");
        assert_eq!(terminal.event, "subscription.resync_required");
        assert!(terminal.reason.contains("non-contiguous assistant preview"));
    }

    fn signed_test_checkpoint(
        entry: &SubscriptionEntry,
        mutate: impl FnOnce(&mut CompositeCheckpoint),
    ) -> String {
        let revision = entry.snapshot();
        let key_revision = checkpoint_key_revision(now_ms());
        let mut checkpoint = CompositeCheckpoint {
            schema_version: LIVE_CONTRACT_SCHEMA_VERSION,
            subscription_id: entry.id.clone(),
            subscription_revision: revision.revision,
            selector_hash: revision.selector_hash.clone(),
            principal_hash: entry.principal_hash.clone(),
            surface_instance_hash: entry.surface_instance_hash.clone(),
            issued_at_ms: now_ms(),
            expires_at_ms: revision.expires_at_ms,
            key_revision,
            source_cursors: BTreeMap::from([("session:a".to_string(), 9)]),
            source_revisions: BTreeMap::from([("session:a".to_string(), 0)]),
        };
        mutate(&mut checkpoint);
        let key = derive_checkpoint_key(&TEST_CHECKPOINT_SECRET, checkpoint.key_revision).unwrap();
        sign_checkpoint(&checkpoint, &key, entry.limits.checkpoint_max_bytes).unwrap()
    }

    #[test]
    fn selector_normalization_rejects_duplicate_source_owners() {
        let source = LiveSourceSelector {
            kind: LiveSourceKind::Session,
            id: "session-1".to_string(),
            cursor: 0,
            revision: 0,
            detail_scope: ProjectionDetailScope::Summary,
        };
        let error = normalize_selector(
            LiveSelector {
                sources: vec![source.clone(), source],
            },
            runtime::GatewayLiveConfig::default().max_sources,
        )
        .unwrap_err();
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn surface_instance_binding_rejects_missing_and_mismatched_callers() {
        let headers = HeaderMap::new();
        assert_eq!(
            require_surface_instance(&headers, None).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            require_surface_instance(&headers, Some("webui:test")).unwrap(),
            "webui:test"
        );

        let mut headers = HeaderMap::new();
        headers.insert(SURFACE_INSTANCE_HEADER, "webui:other".parse().unwrap());
        assert_eq!(
            require_surface_instance(&headers, Some("webui:test"))
                .unwrap_err()
                .0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            require_surface_instance(&headers, None).unwrap(),
            "webui:other"
        );
    }

    #[test]
    fn checkpoint_signature_detects_tampering() {
        let checkpoint = CompositeCheckpoint {
            schema_version: LIVE_CONTRACT_SCHEMA_VERSION,
            subscription_id: "subscription".to_string(),
            subscription_revision: 1,
            selector_hash: "selector".to_string(),
            principal_hash: "principal".to_string(),
            surface_instance_hash: "surface".to_string(),
            issued_at_ms: now_ms(),
            expires_at_ms: now_ms() + 60_000,
            key_revision: checkpoint_key_revision(now_ms()),
            source_cursors: BTreeMap::from([("session:a".to_string(), 9)]),
            source_revisions: BTreeMap::from([("session:a".to_string(), 0)]),
        };
        let token = sign_checkpoint(&checkpoint, &[7; 32], 6_144).unwrap();
        let (payload, signature) = token.split_once('.').unwrap();
        let mut bad_signature = signature.as_bytes().to_vec();
        bad_signature[0] = if bad_signature[0] == b'A' { b'B' } else { b'A' };
        let bad_signature = URL_SAFE_NO_PAD
            .decode(String::from_utf8(bad_signature).unwrap())
            .unwrap();
        let mut mac = HmacSha256::new_from_slice(&[7; 32]).unwrap();
        mac.update(payload.as_bytes());
        assert!(mac.verify_slice(&bad_signature).is_err());
    }

    #[test]
    fn checkpoint_keys_rotate_by_revision_without_reusing_key_material() {
        let master = [11; 32];
        let current = checkpoint_key_revision(now_ms());
        let current_key = derive_checkpoint_key(&master, current).unwrap();
        let next_key = derive_checkpoint_key(&master, current.saturating_add(1)).unwrap();
        assert_ne!(current_key, next_key);
        assert!(checkpoint_key_retention_revisions(86_400) >= 4);
    }

    #[test]
    fn checkpoint_verification_fails_closed_for_every_binding_boundary() {
        let entry = test_entry(LiveSelector {
            sources: vec![test_source("a")],
        });
        let valid = signed_test_checkpoint(&entry, |_| {});
        assert!(
            verify_checkpoint(&valid, &entry, &entry.snapshot(), &TEST_CHECKPOINT_SECRET).is_ok()
        );
        let retained_rotated_key = signed_test_checkpoint(&entry, |checkpoint| {
            checkpoint.key_revision = checkpoint.key_revision.saturating_sub(1);
        });
        assert!(verify_checkpoint(
            &retained_rotated_key,
            &entry,
            &entry.snapshot(),
            &TEST_CHECKPOINT_SECRET
        )
        .is_ok());

        let mismatched_principal =
            signed_test_checkpoint(&entry, |checkpoint| checkpoint.principal_hash.push('x'));
        assert!(verify_checkpoint(
            &mismatched_principal,
            &entry,
            &entry.snapshot(),
            &TEST_CHECKPOINT_SECRET,
        )
        .unwrap_err()
        .contains("mismatched"));

        let expired = signed_test_checkpoint(&entry, |checkpoint| {
            checkpoint.expires_at_ms = now_ms().saturating_sub(1);
        });
        assert!(
            verify_checkpoint(&expired, &entry, &entry.snapshot(), &TEST_CHECKPOINT_SECRET)
                .unwrap_err()
                .contains("mismatched")
        );

        let issued_in_the_future = signed_test_checkpoint(&entry, |checkpoint| {
            checkpoint.issued_at_ms = now_ms().saturating_add(60_000);
        });
        assert!(verify_checkpoint(
            &issued_in_the_future,
            &entry,
            &entry.snapshot(),
            &TEST_CHECKPOINT_SECRET,
        )
        .unwrap_err()
        .contains("mismatched"));

        let foreign_source = signed_test_checkpoint(&entry, |checkpoint| {
            checkpoint
                .source_cursors
                .insert("session:foreign".to_string(), 1);
        });
        assert!(verify_checkpoint(
            &foreign_source,
            &entry,
            &entry.snapshot(),
            &TEST_CHECKPOINT_SECRET,
        )
        .unwrap_err()
        .contains("unselected"));

        let future_key = signed_test_checkpoint(&entry, |checkpoint| {
            checkpoint.key_revision = checkpoint.key_revision.saturating_add(1);
        });
        assert!(verify_checkpoint(
            &future_key,
            &entry,
            &entry.snapshot(),
            &TEST_CHECKPOINT_SECRET
        )
        .unwrap_err()
        .contains("no longer valid"));

        let retired_key = signed_test_checkpoint(&entry, |checkpoint| {
            checkpoint.key_revision = checkpoint.key_revision.saturating_sub(
                checkpoint_key_retention_revisions(entry.limits.max_ttl_seconds).saturating_add(1),
            );
        });
        assert!(verify_checkpoint(
            &retired_key,
            &entry,
            &entry.snapshot(),
            &TEST_CHECKPOINT_SECRET
        )
        .unwrap_err()
        .contains("no longer valid"));
    }

    #[test]
    fn checkpoint_size_overflow_is_never_silently_truncated() {
        let checkpoint = CompositeCheckpoint {
            schema_version: LIVE_CONTRACT_SCHEMA_VERSION,
            subscription_id: "subscription".to_string(),
            subscription_revision: 1,
            selector_hash: "selector".repeat(32),
            principal_hash: "principal".repeat(32),
            surface_instance_hash: "surface".repeat(32),
            issued_at_ms: now_ms(),
            expires_at_ms: now_ms() + 60_000,
            key_revision: checkpoint_key_revision(now_ms()),
            source_cursors: (0..64)
                .map(|index| (format!("session:{index:04}"), index))
                .collect(),
            source_revisions: BTreeMap::new(),
        };
        let error = sign_checkpoint(&checkpoint, &[3; 32], 128).unwrap_err();
        assert!(error.contains("exceeds"));
    }

    #[tokio::test]
    async fn physical_stream_rejects_queued_envelopes_from_an_old_revision() {
        let entry = test_entry(LiveSelector {
            sources: vec![test_source("a")],
        });
        let current = entry.snapshot();
        entry.revisions.send_replace(Arc::new(SubscriptionRevision {
            revision: 2,
            selector: current.selector.clone(),
            selector_hash: current.selector_hash.clone(),
            expires_at_ms: current.expires_at_ms,
            deleted: false,
        }));
        let (tx, rx) = mpsc::channel(4);
        tx.send(QueuedEnvelope {
            envelope: envelope(
                &entry,
                1,
                "session",
                "a",
                ProjectionDetailScope::Summary,
                Some(1),
                DeliveryClass::Durable,
                SourceHealth::Live,
                "old",
                serde_json::Value::Null,
            ),
            checkpoint_update: Some(("session:a".to_string(), 1, 0)),
        })
        .await
        .unwrap();
        tx.send(QueuedEnvelope {
            envelope: envelope(
                &entry,
                2,
                "session",
                "a",
                ProjectionDetailScope::Summary,
                Some(2),
                DeliveryClass::Durable,
                SourceHealth::Live,
                "current",
                serde_json::Value::Null,
            ),
            checkpoint_update: Some(("session:a".to_string(), 2, 7)),
        })
        .await
        .unwrap();
        drop(tx);
        let mut stream = PhysicalLiveStream {
            rx: ReceiverStream::new(rx),
            entry,
            terminal: Arc::new(Mutex::new(None)),
            delivered_cursors: Arc::new(Mutex::new(BTreeMap::new())),
            delivered_revisions: Arc::new(Mutex::new(BTreeMap::new())),
            checkpoint_secret: TEST_CHECKPOINT_SECRET,
            ready_revision: 0,
            ended: false,
        };
        let event = stream.next().await.unwrap().unwrap();
        let rendered = format!("{event:?}");
        assert!(rendered.contains("current"));
        assert!(!rendered.contains("old"));
        assert_eq!(
            stream
                .delivered_revisions
                .lock()
                .unwrap()
                .get("session:a")
                .copied(),
            Some(7),
            "the signed resume checkpoint must retain the applied projection revision"
        );
    }

    #[tokio::test]
    async fn physical_stream_keeps_ready_revision_until_replacement_barrier() {
        let entry = test_entry(LiveSelector {
            sources: vec![test_source("a")],
        });
        let (tx, rx) = mpsc::channel(8);
        tx.send(QueuedEnvelope {
            envelope: envelope(
                &entry,
                1,
                "subscription",
                "subscription-a",
                ProjectionDetailScope::Summary,
                None,
                DeliveryClass::SnapshotReconstructable,
                SourceHealth::Baseline,
                "subscription.ready",
                serde_json::Value::Null,
            ),
            checkpoint_update: None,
        })
        .await
        .unwrap();
        let mut stream = PhysicalLiveStream {
            rx: ReceiverStream::new(rx),
            entry: Arc::clone(&entry),
            terminal: Arc::new(Mutex::new(None)),
            delivered_cursors: Arc::new(Mutex::new(BTreeMap::new())),
            delivered_revisions: Arc::new(Mutex::new(BTreeMap::new())),
            checkpoint_secret: TEST_CHECKPOINT_SECRET,
            ready_revision: 0,
            ended: false,
        };
        let first = stream.next().await.unwrap().unwrap();
        assert!(format!("{first:?}").contains("subscription.ready"));
        assert_eq!(stream.ready_revision, 1);

        let current = entry.snapshot();
        entry.revisions.send_replace(Arc::new(SubscriptionRevision {
            revision: 2,
            selector: current.selector.clone(),
            selector_hash: current.selector_hash.clone(),
            expires_at_ms: current.expires_at_ms,
            deleted: false,
        }));
        tx.send(QueuedEnvelope {
            envelope: envelope(
                &entry,
                1,
                "session",
                "a",
                ProjectionDetailScope::Summary,
                None,
                DeliveryClass::EphemeralPreview,
                SourceHealth::Live,
                "old-before-barrier",
                serde_json::Value::Null,
            ),
            checkpoint_update: None,
        })
        .await
        .unwrap();
        let old_before_barrier = stream.next().await.unwrap().unwrap();
        assert!(format!("{old_before_barrier:?}").contains("old-before-barrier"));

        tx.send(QueuedEnvelope {
            envelope: envelope(
                &entry,
                2,
                "subscription",
                "subscription-a",
                ProjectionDetailScope::Summary,
                None,
                DeliveryClass::SnapshotReconstructable,
                SourceHealth::Baseline,
                "subscription.revision.changed",
                serde_json::Value::Null,
            ),
            checkpoint_update: None,
        })
        .await
        .unwrap();
        let replacement_barrier = stream.next().await.unwrap().unwrap();
        assert!(format!("{replacement_barrier:?}").contains("subscription.revision.changed"));
        assert_eq!(stream.ready_revision, 2);

        tx.send(QueuedEnvelope {
            envelope: envelope(
                &entry,
                1,
                "session",
                "a",
                ProjectionDetailScope::Summary,
                None,
                DeliveryClass::EphemeralPreview,
                SourceHealth::Live,
                "old-after-barrier",
                serde_json::Value::Null,
            ),
            checkpoint_update: None,
        })
        .await
        .unwrap();
        tx.send(QueuedEnvelope {
            envelope: envelope(
                &entry,
                2,
                "session",
                "a",
                ProjectionDetailScope::Summary,
                None,
                DeliveryClass::EphemeralPreview,
                SourceHealth::Live,
                "current-after-barrier",
                serde_json::Value::Null,
            ),
            checkpoint_update: None,
        })
        .await
        .unwrap();
        drop(tx);
        let current_after_barrier = stream.next().await.unwrap().unwrap();
        let rendered = format!("{current_after_barrier:?}");
        assert!(rendered.contains("current-after-barrier"));
        assert!(!rendered.contains("old-after-barrier"));
    }

    #[tokio::test]
    async fn concurrent_patch_is_atomic_and_idempotent() {
        let entry = test_entry(LiveSelector {
            sources: vec![test_source("a")],
        });
        let first_selector = LiveSelector {
            sources: vec![test_source("first")],
        };
        let second_selector = LiveSelector {
            sources: vec![test_source("second")],
        };
        let first_hash = patch_request_hash(1, &first_selector, None);
        let second_hash = patch_request_hash(1, &second_selector, None);
        let barrier = Arc::new(tokio::sync::Barrier::new(3));

        let first_entry = Arc::clone(&entry);
        let first_barrier = Arc::clone(&barrier);
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            patch_live_subscription_entry(
                &first_entry,
                "patch-first".to_string(),
                1,
                first_selector,
                None,
                first_hash,
            )
            .await
        });
        let second_entry = Arc::clone(&entry);
        let second_barrier = Arc::clone(&barrier);
        let second = tokio::spawn(async move {
            second_barrier.wait().await;
            patch_live_subscription_entry(
                &second_entry,
                "patch-second".to_string(),
                1,
                second_selector,
                None,
                second_hash,
            )
            .await
        });
        barrier.wait().await;

        let results = [first.await.unwrap(), second.await.unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| result
                    .as_ref()
                    .is_err_and(|error| error.0 == StatusCode::CONFLICT))
                .count(),
            1
        );
        assert_eq!(entry.snapshot().revision, 2);

        let accepted = results
            .into_iter()
            .find_map(Result::ok)
            .expect("one PATCH must win")
            .0;
        let accepted_hash = patch_request_hash(1, &accepted.selector, None);
        let replay = patch_live_subscription_entry(
            &entry,
            if accepted.selector.sources[0].id == "first" {
                "patch-first".to_string()
            } else {
                "patch-second".to_string()
            },
            1,
            accepted.selector.clone(),
            None,
            accepted_hash,
        )
        .await
        .expect("same idempotency key and request must replay")
        .0;
        assert_eq!(replay, accepted);
    }
}
const TEST_CHECKPOINT_SECRET: [u8; 32] = [37; 32];
