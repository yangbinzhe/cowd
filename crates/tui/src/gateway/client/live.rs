use super::*;
impl TuiLiveMultiplexer {
    pub(super) async fn subscribe(
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

pub(super) fn refresh_tui_live_source_selector(source: &mut LiveSourceState) {
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

pub(super) async fn deliver_tui_live_resync(
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

pub(super) async fn deliver_tui_live_envelope(
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
            &crate::gateway_client_routes::runtime::for_runtime_entity(
                surface::gateway_api::paths::API_RUNTIME_LIVE_SUBSCRIPTIONS_BY_ID,
                active.id.to_string(),
            ),
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
        surface::gateway_api::paths::API_RUNTIME_LIVE_SUBSCRIPTIONS.template(),
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
