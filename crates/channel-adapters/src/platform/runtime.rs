//! Platform runtime for managing multiple platform adapters.

use crate::mirror::MessageMirror;
use crate::platform::adapter::{
    InboundMessage, OutboundDispatch, OutboundMessage, PlatformAdapter, PlatformError,
};
use crate::platform::config::{PlatformRuntimeConfig, RetryConfig, SessionResetPolicy};
use crate::platform::types::{PlatformSession, SendResult, SessionKey};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Handle to a running adapter loop, allowing responses to be sent back.
struct AdapterHandle {
    /// Channel for sending outbound messages to this adapter's loop.
    outbound_tx: mpsc::Sender<RuntimeOutboundCommand>,
}

enum RuntimeOutboundCommand {
    Enqueue(OutboundMessage),
    Dispatch {
        dispatch: OutboundDispatch,
        ack: tokio::sync::oneshot::Sender<Result<SendResult, PlatformError>>,
    },
}

/// Platform runtime that manages all registered adapters.
pub struct PlatformRuntime {
    /// Configuration.
    config: PlatformRuntimeConfig,
    /// Active adapters keyed by platform name (held until start() is called).
    adapters: RwLock<HashMap<String, Box<dyn PlatformAdapter>>>,
    /// Active sessions keyed by session key.
    sessions: RwLock<HashMap<SessionKey, PlatformSession>>,
    /// Channel for receiving inbound messages.
    message_rx: RwLock<Option<mpsc::Receiver<InboundMessage>>>,
    /// Shutdown signal.
    shutdown_tx: RwLock<Option<tokio::sync::broadcast::Sender<()>>>,
    /// Handles to running adapter loops for sending responses.
    adapter_handles: RwLock<HashMap<String, AdapterHandle>>,
    /// Optional message mirror for cross-platform session synchronization.
    message_mirror: RwLock<Option<Arc<MessageMirror>>>,
}

impl PlatformRuntime {
    /// Create a new platform runtime.
    pub fn new(config: PlatformRuntimeConfig) -> Self {
        Self {
            config,
            adapters: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            message_rx: RwLock::new(None),
            shutdown_tx: RwLock::new(None),
            adapter_handles: RwLock::new(HashMap::new()),
            message_mirror: RwLock::new(None),
        }
    }

    /// Register a platform adapter.
    pub async fn register_adapter(
        &self,
        adapter: Box<dyn PlatformAdapter>,
    ) -> Result<(), PlatformError> {
        let platform_name = adapter.platform_name().to_string();
        let mut adapters = self.adapters.write().await;
        adapters.insert(platform_name, adapter);
        Ok(())
    }

    /// Set the message mirror for cross-platform session synchronization.
    pub async fn set_mirror(&self, mirror: Arc<MessageMirror>) {
        *self.message_mirror.write().await = Some(mirror);
    }

    /// List all registered platform adapter names.
    pub async fn list_platforms(&self) -> Vec<String> {
        let adapters = self.adapters.read().await;
        adapters.keys().cloned().collect()
    }

    /// List adapter names that are currently bound to a running adapter loop.
    pub async fn list_bound_adapters(&self) -> Vec<String> {
        let handles = self.adapter_handles.read().await;
        let mut names = handles.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    /// Check whether a platform has a running adapter handle.
    pub async fn has_bound_adapter(&self, platform_name: &str) -> bool {
        let handles = self.adapter_handles.read().await;
        handles.contains_key(platform_name)
    }

    /// Get platform info by name.
    pub async fn get_platform_info(&self, name: &str) -> Option<serde_json::Value> {
        let adapters = self.adapters.read().await;
        let adapter = adapters.get(name)?;
        Some(serde_json::json!({
            "name": adapter.platform_name(),
            "platform": format!("{:?}", adapter.platform()),
        }))
    }

    /// List active sessions.
    pub async fn list_sessions(&self) -> Vec<serde_json::Value> {
        let sessions = self.sessions.read().await;
        sessions
            .values()
            .map(|s| {
                serde_json::json!({
                    "session_key": s.key.as_str(),
                    "platform": &s.key.platform,
                    "user_id": &s.key.user_id,
                    "thread_id": &s.key.thread_id,
                    "created_at": s.created_at.to_rfc3339(),
                    "last_activity": s.last_activity.to_rfc3339(),
                })
            })
            .collect()
    }

    /// Get session count for a specific platform.
    pub async fn platform_session_count(&self, platform: &str) -> usize {
        let sessions = self.sessions.read().await;
        sessions
            .values()
            .filter(|s| s.key.platform == platform)
            .count()
    }

    /// Delete a session.
    pub async fn delete_session(&self, session_key: &str) -> bool {
        let mut sessions = self.sessions.write().await;
        // Try to parse as SessionKey
        sessions.remove(&SessionKey::from(session_key)).is_some()
    }

    /// Start the platform runtime.
    ///
    /// This connects all registered adapters and starts the receive loop.
    /// After start, adapters are moved into spawned tasks; use `send_response`
    /// to reply through the adapter's outbound channel.
    pub async fn start(&self) -> Result<(), PlatformError> {
        let (inbound_tx, inbound_rx) =
            mpsc::channel::<InboundMessage>(self.config.channel_capacity);
        *self.message_rx.write().await = Some(inbound_rx);

        // Connect all adapters in parallel
        let mut adapters = self.adapters.write().await;
        let adapter_entries: Vec<(String, Box<dyn PlatformAdapter>)> = adapters.drain().collect();
        drop(adapters); // release the write lock

        let connect_handles: Vec<_> = adapter_entries
            .into_iter()
            .map(|(name, mut adapter)| {
                tokio::spawn(async move {
                    info!(platform = %name, "connecting platform adapter");
                    let result = adapter.connect().await;
                    (name, adapter, result)
                })
            })
            .collect();

        let connect_results = futures::future::join_all(connect_handles).await;

        // Process results — log errors and continue with remaining adapters
        let mut connected_adapters = Vec::new();
        for result in connect_results {
            match result {
                Ok((name, adapter, Ok(()))) => {
                    debug!(platform = %name, "platform adapter connected");
                    connected_adapters.push((name, adapter));
                }
                Ok((name, _adapter, Err(e))) => {
                    error!(platform = %name, error = %e, "failed to connect platform adapter");
                }
                Err(e) => {
                    error!(error = %e, "task join error");
                }
            }
        }

        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
        *self.shutdown_tx.write().await = Some(shutdown_tx.clone());

        let mirror = self.message_mirror.read().await.clone();
        let outbound_senders: Arc<
            std::sync::Mutex<HashMap<String, mpsc::Sender<RuntimeOutboundCommand>>>,
        > = Arc::new(std::sync::Mutex::new(HashMap::new()));

        for (platform_name, adapter) in connected_adapters {
            let (outbound_tx, outbound_rx) =
                mpsc::channel::<RuntimeOutboundCommand>(self.config.channel_capacity);
            self.adapter_handles.write().await.insert(
                platform_name.clone(),
                AdapterHandle {
                    outbound_tx: outbound_tx.clone(),
                },
            );
            outbound_senders
                .lock()
                .unwrap()
                .insert(platform_name.clone(), outbound_tx);

            let inbound_tx_clone = inbound_tx.clone();
            let shutdown_rx = shutdown_tx.subscribe();
            let retry_config = self.config.retry.clone();
            let mirror = mirror.clone();
            let outbound_senders = outbound_senders.clone();

            tokio::spawn(async move {
                run_adapter_loop(
                    platform_name,
                    adapter,
                    inbound_tx_clone,
                    outbound_rx,
                    shutdown_rx,
                    retry_config,
                    mirror,
                    outbound_senders,
                )
                .await;
            });
        }

        info!("platform runtime started");
        Ok(())
    }

    /// Get the next inbound message.
    ///
    /// Returns `None` if the channel is closed.
    pub async fn next_message(&self) -> Result<Option<InboundMessage>, PlatformError> {
        let mut rx = self.message_rx.write().await;
        if let Some(rx) = rx.as_mut() {
            Ok(rx.recv().await)
        } else {
            Ok(None)
        }
    }

    /// Send a response to a platform via its outbound channel.
    pub async fn send_response(
        &self,
        platform_name: &str,
        msg: OutboundMessage,
    ) -> Result<(), PlatformError> {
        let handles = self.adapter_handles.read().await;
        let handle = handles.get(platform_name).ok_or_else(|| {
            PlatformError::Unknown(format!("no adapter handle for platform: {platform_name}"))
        })?;

        handle
            .outbound_tx
            .send(RuntimeOutboundCommand::Enqueue(msg))
            .await
            .map_err(|e| PlatformError::SendFailed(format!("outbound channel closed: {e}")))
    }

    /// Dispatch a text message and wait until the adapter send call completes.
    pub async fn dispatch_outbound(
        &self,
        platform_name: &str,
        msg: OutboundMessage,
    ) -> Result<SendResult, PlatformError> {
        self.dispatch_payload(platform_name, OutboundDispatch::text(msg))
            .await
    }

    /// Dispatch a typed payload and wait until the adapter send call completes.
    pub async fn dispatch_payload(
        &self,
        platform_name: &str,
        dispatch: OutboundDispatch,
    ) -> Result<SendResult, PlatformError> {
        let handles = self.adapter_handles.read().await;
        let handle = handles.get(platform_name).ok_or_else(|| {
            PlatformError::Unknown(format!("no adapter handle for platform: {platform_name}"))
        })?;
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();

        handle
            .outbound_tx
            .send(RuntimeOutboundCommand::Dispatch {
                dispatch,
                ack: ack_tx,
            })
            .await
            .map_err(|e| PlatformError::SendFailed(format!("outbound channel closed: {e}")))?;
        ack_rx
            .await
            .map_err(|e| PlatformError::SendFailed(format!("dispatch ack closed: {e}")))?
    }

    /// Get or create a session.
    pub async fn get_session(
        &self,
        key: SessionKey,
    ) -> Arc<RwLock<crate::platform::types::PlatformSession>> {
        let mut sessions = self.sessions.write().await;
        if !sessions.contains_key(&key) {
            sessions.insert(
                key.clone(),
                crate::platform::types::PlatformSession::new(key.clone()),
            );
        }
        Arc::new(RwLock::new(sessions.get(&key).unwrap().clone()))
    }

    /// Check if a session exists.
    pub async fn has_session(&self, key: &SessionKey) -> bool {
        let sessions = self.sessions.read().await;
        sessions.contains_key(key)
    }

    /// Remove expired sessions based on the reset policy.
    pub async fn cleanup_sessions(&self) {
        let policy = self.config.session_reset;
        if matches!(policy, SessionResetPolicy::None) {
            return;
        }

        let mut sessions = self.sessions.write().await;
        let now = chrono::Utc::now();
        let idle_duration = chrono::Duration::minutes(self.config.idle_timeout_minutes);

        sessions.retain(|_key, session| match policy {
            SessionResetPolicy::Always => false,
            SessionResetPolicy::Daily => session.created_at.date_naive() == now.date_naive(),
            SessionResetPolicy::Idle => (now - session.last_activity) < idle_duration,
            SessionResetPolicy::Both => {
                session.created_at.date_naive() == now.date_naive()
                    && (now - session.last_activity) < idle_duration
            }
            SessionResetPolicy::None => true,
        });
    }

    /// Shutdown the platform runtime gracefully.
    pub async fn shutdown(&self) -> Result<(), PlatformError> {
        info!("shutting down platform runtime");

        // Signal shutdown
        if let Some(tx) = self.shutdown_tx.write().await.take() {
            let _ = tx.send(());
        }

        // Clear adapter handles (this will cause the outbound channels to close,
        // which signals the adapter loops to finish their current work).
        self.adapter_handles.write().await.clear();

        info!("platform runtime shutdown complete");
        Ok(())
    }
}

/// Run the receive+send loop for a single adapter.
///
/// The adapter owns both receive and send. Inbound messages are forwarded
/// through `inbound_tx`; outbound messages arrive via `outbound_rx`.
async fn run_adapter_loop(
    platform_name: String,
    mut adapter: Box<dyn PlatformAdapter>,
    inbound_tx: mpsc::Sender<InboundMessage>,
    mut outbound_rx: mpsc::Receiver<RuntimeOutboundCommand>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    retry_config: RetryConfig,
    mirror: Option<Arc<MessageMirror>>,
    outbound_senders: Arc<std::sync::Mutex<HashMap<String, mpsc::Sender<RuntimeOutboundCommand>>>>,
) {
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!(platform = %platform_name, "shutdown received");
                break;
            }
            result = adapter.receive() => {
                match result {
                    Ok(Some(msg)) => {
                        // Mirror the message to target sessions before forwarding
                        if let Some(ref mirror) = mirror {
                            let mirrored = mirror.mirror(&msg).await;
                            for m in mirrored {
                                if let Some(ref target_platform) = m.target_platform {
                                    let sender = {
                                        let senders = outbound_senders.lock().unwrap();
                                        senders.get(target_platform.as_str()).cloned()
                                    };
                                    if let Some(tx) = sender {
                                        let out_msg = OutboundMessage {
                                            session_key: SessionKey::from(m.target_session.as_str()),
                                            text: m.content.clone(),
                                            reply_to: None,
                                            metadata: serde_json::json!({"mirror": true}),
                                        };
                                        let _ = tx
                                            .send(RuntimeOutboundCommand::Enqueue(out_msg))
                                            .await;
                                    }
                                }
                            }
                        }

                        if inbound_tx.send(msg).await.is_err() {
                            warn!(platform = %platform_name, "inbound receiver dropped, stopping");
                            break;
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        warn!(platform = %platform_name, error = %e, "receive error, retrying");
                        let mut retry_count = 0;
                        while retry_count < retry_config.max_retries {
                            tokio::time::sleep(tokio::time::Duration::from_millis(
                                retry_config.initial_delay_ms * (2_u64.pow(retry_count as u32))
                            )).await;
                            match adapter.receive().await {
                                Ok(Some(_)) | Err(_) => break,
                                Ok(None) => retry_count += 1,
                            }
                        }
                    }
                }
            }
            Some(command) = outbound_rx.recv() => {
                match command {
                    RuntimeOutboundCommand::Enqueue(out_msg) => {
                        if let Err(e) = adapter.send(&out_msg).await {
                            warn!(platform = %platform_name, error = %e, "failed to send outbound message");
                        }
                    }
                    RuntimeOutboundCommand::Dispatch { dispatch, ack } => {
                        let result = dispatch_adapter_payload(adapter.as_ref(), &dispatch).await;
                        if let Err(e) = &result {
                            warn!(platform = %platform_name, error = %e, "failed to dispatch outbound message");
                        }
                        let _ = ack.send(result);
                    }
                }
            }
        }
    }

    // Disconnect adapter on exit
    if let Err(e) = adapter.disconnect().await {
        warn!(platform = %platform_name, error = %e, "error disconnecting adapter on loop exit");
    }
    info!(platform = %platform_name, "adapter loop exited");
}

async fn dispatch_adapter_payload(
    adapter: &dyn PlatformAdapter,
    dispatch: &OutboundDispatch,
) -> Result<SendResult, PlatformError> {
    let chat_id = dispatch
        .session_key
        .thread_id
        .as_deref()
        .unwrap_or(&dispatch.session_key.user_id);
    match dispatch.kind {
        crate::platform::types::OutboundPayloadKind::Text => {
            adapter
                .send(&OutboundMessage {
                    session_key: dispatch.session_key.clone(),
                    text: dispatch.payload_ref.clone(),
                    reply_to: dispatch.reply_to.clone(),
                    metadata: dispatch.metadata.clone(),
                })
                .await
        }
        crate::platform::types::OutboundPayloadKind::Image => {
            if dispatch.payload_ref.starts_with("http://")
                || dispatch.payload_ref.starts_with("https://")
            {
                adapter
                    .send_image(chat_id, &dispatch.payload_ref, dispatch.caption.as_deref())
                    .await?;
            } else {
                adapter
                    .send_image_file(chat_id, &dispatch.payload_ref, dispatch.caption.as_deref())
                    .await?;
            }
            Ok(SendResult::success(None))
        }
        crate::platform::types::OutboundPayloadKind::File => {
            adapter
                .send_document(
                    chat_id,
                    &dispatch.payload_ref,
                    dispatch.file_name.as_deref(),
                    dispatch.caption.as_deref(),
                )
                .await?;
            Ok(SendResult::success(None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::types::Platform;
    use async_trait::async_trait;

    struct MockAdapter {
        name: String,
        connected: bool,
        sent: Arc<std::sync::Mutex<Vec<OutboundMessage>>>,
        media_sent: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl MockAdapter {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                connected: false,
                sent: Arc::new(std::sync::Mutex::new(Vec::new())),
                media_sent: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn new_with_sent(name: &str, sent: Arc<std::sync::Mutex<Vec<OutboundMessage>>>) -> Self {
            Self {
                name: name.to_string(),
                connected: false,
                sent,
                media_sent: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn new_with_media(name: &str, media_sent: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
            Self {
                name: name.to_string(),
                connected: false,
                sent: Arc::new(std::sync::Mutex::new(Vec::new())),
                media_sent,
            }
        }
    }

    #[async_trait]
    impl PlatformAdapter for MockAdapter {
        fn platform(&self) -> Platform {
            Platform::Custom(self.name.clone())
        }

        fn platform_name(&self) -> &str {
            &self.name
        }

        async fn connect(&mut self) -> Result<(), PlatformError> {
            self.connected = true;
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<(), PlatformError> {
            self.connected = false;
            Ok(())
        }

        fn is_connected(&self) -> bool {
            self.connected
        }

        async fn receive(&mut self) -> Result<Option<InboundMessage>, PlatformError> {
            Ok(None)
        }

        async fn send(&self, msg: &OutboundMessage) -> Result<SendResult, PlatformError> {
            self.sent.lock().unwrap().push(msg.clone());
            Ok(SendResult::success(Some(format!(
                "mock-{}",
                msg.session_key.user_id
            ))))
        }

        async fn send_image(
            &self,
            chat_id: &str,
            image_url: &str,
            caption: Option<&str>,
        ) -> Result<(), PlatformError> {
            self.media_sent.lock().unwrap().push(format!(
                "image-url:{chat_id}:{image_url}:{}",
                caption.unwrap_or("")
            ));
            Ok(())
        }

        async fn send_image_file(
            &self,
            chat_id: &str,
            image_path: &str,
            caption: Option<&str>,
        ) -> Result<(), PlatformError> {
            self.media_sent.lock().unwrap().push(format!(
                "image-file:{chat_id}:{image_path}:{}",
                caption.unwrap_or("")
            ));
            Ok(())
        }

        async fn send_document(
            &self,
            chat_id: &str,
            file_path: &str,
            file_name: Option<&str>,
            caption: Option<&str>,
        ) -> Result<(), PlatformError> {
            self.media_sent.lock().unwrap().push(format!(
                "file:{chat_id}:{file_path}:{}:{}",
                file_name.unwrap_or(""),
                caption.unwrap_or("")
            ));
            Ok(())
        }
    }

    #[tokio::test]
    async fn bound_adapter_snapshot_tracks_started_and_shutdown_runtime() {
        let runtime = PlatformRuntime::new(PlatformRuntimeConfig::default());
        runtime
            .register_adapter(Box::new(MockAdapter::new("feishu")))
            .await
            .unwrap();

        assert!(!runtime.has_bound_adapter("feishu").await);
        assert!(runtime.list_bound_adapters().await.is_empty());

        runtime.start().await.unwrap();
        assert!(runtime.has_bound_adapter("feishu").await);
        assert_eq!(runtime.list_bound_adapters().await, vec!["feishu"]);

        runtime.shutdown().await.unwrap();
        assert!(!runtime.has_bound_adapter("feishu").await);
        assert!(runtime.list_bound_adapters().await.is_empty());
    }

    #[tokio::test]
    async fn dispatch_outbound_waits_for_adapter_send() {
        let runtime = PlatformRuntime::new(PlatformRuntimeConfig::default());
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        runtime
            .register_adapter(Box::new(MockAdapter::new_with_sent("feishu", sent.clone())))
            .await
            .unwrap();
        runtime.start().await.unwrap();

        let result = runtime
            .dispatch_outbound(
                "feishu",
                OutboundMessage {
                    session_key: SessionKey::new("feishu", "open-id"),
                    text: "hello live".to_string(),
                    reply_to: None,
                    metadata: serde_json::json!({"test": true}),
                },
            )
            .await
            .unwrap();
        assert_eq!(result.message_id.as_deref(), Some("mock-open-id"));

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].session_key.as_str(), "feishu:open-id");
        assert_eq!(sent[0].text, "hello live");

        drop(sent);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn dispatch_payload_routes_image_and_file_to_adapter_methods() {
        let runtime = PlatformRuntime::new(PlatformRuntimeConfig::default());
        let media_sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        runtime
            .register_adapter(Box::new(MockAdapter::new_with_media(
                "feishu",
                media_sent.clone(),
            )))
            .await
            .unwrap();
        runtime.start().await.unwrap();

        runtime
            .dispatch_payload(
                "feishu",
                OutboundDispatch {
                    session_key: SessionKey::with_thread("feishu", "open-id", "chat-id"),
                    kind: crate::platform::types::OutboundPayloadKind::Image,
                    payload_ref: "https://example.test/image.png".to_string(),
                    caption: Some("diagram".to_string()),
                    file_name: None,
                    reply_to: None,
                    metadata: serde_json::json!({"test": true}),
                },
            )
            .await
            .unwrap();
        runtime
            .dispatch_payload(
                "feishu",
                OutboundDispatch {
                    session_key: SessionKey::new("feishu", "open-id"),
                    kind: crate::platform::types::OutboundPayloadKind::File,
                    payload_ref: "/tmp/report.pdf".to_string(),
                    caption: None,
                    file_name: Some("report.pdf".to_string()),
                    reply_to: None,
                    metadata: serde_json::json!({"test": true}),
                },
            )
            .await
            .unwrap();

        let media_sent = media_sent.lock().unwrap();
        assert_eq!(
            media_sent.as_slice(),
            [
                "image-url:chat-id:https://example.test/image.png:diagram",
                "file:open-id:/tmp/report.pdf:report.pdf:"
            ]
        );
        drop(media_sent);
        runtime.shutdown().await.unwrap();
    }
}
