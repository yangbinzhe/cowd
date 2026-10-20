use super::*;

#[cfg(test)]
mod receipt_recovery_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires a parent-owned PostgreSQL producer allocation and prebuilt step binary"]
    async fn r1_client_receipt_ack_response_loss_restores_exact_pending() {
        let scenario = super::super::tests::RealReceiptScenario::load("tui-loss").await;
        let (client, server) = scenario.fixture(true).await;
        client
            .attach_session(scenario.session(), "tui", Some("reader"))
            .await
            .unwrap();
        let (tx, mut receiver) = crate::cowd_event_channel();
        scenario.publish(&client, &tx).await;
        let mut app = crate::app::App::new("integration", scenario.session());
        while let Ok(event) = receiver.try_recv() {
            app.apply_event(event);
        }
        assert!(client
            .recover_terminal_receipt(scenario.session(), scenario.message())
            .await
            .is_err());
        assert_eq!(scenario.acknowledgements.load(Ordering::SeqCst), 1);
        while let Ok(event) = receiver.try_recv() {
            app.apply_event(event);
        }
        assert!(app.terminal_receipt.is_none());
        assert!(super::super::tests::real_receipt_panel(&app).contains("This client unknown"));
        let overlay = client
            .recover_terminal_receipt(scenario.session(), scenario.message())
            .await
            .unwrap();
        assert!(overlay.received);
        scenario.publish(&client, &tx).await;
        while let Ok(event) = receiver.try_recv() {
            app.apply_event(event);
        }
        assert!(super::super::tests::real_receipt_panel(&app).contains("received (fresh)"));
        scenario.assert_old().await;
        scenario.step("usage-only", false).await;
        assert!(
            client
                .recover_terminal_receipt(scenario.session(), scenario.message())
                .await
                .unwrap()
                .received
        );
        scenario.fresh_transport_unknown().await;
        let saved = scenario.saved_files();
        scenario.step("body-b", false).await;
        assert!(client
            .recover_terminal_receipt(scenario.session(), scenario.message())
            .await
            .is_err());
        scenario.step("restore-body-a", false).await;
        scenario.reject_old(&client).await;
        assert!(client
            .recover_terminal_receipt(scenario.session(), scenario.message())
            .await
            .is_err());
        scenario.expire_locally(&client).await;
        while let Ok(event) = receiver.try_recv() {
            app.apply_event(event);
        }
        assert!(super::super::tests::real_receipt_panel(&app).contains("This client unknown"));
        scenario.fresh_transport_unknown().await;
        scenario.assert_old().await;
        assert_eq!(scenario.saved_files(), saved);
        scenario.finish(&app).await;
        server.abort();
    }
}
use sha2::{Digest, Sha256};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

#[derive(Debug, Clone)]
pub(super) struct ReceiptGrant {
    pub client_id: String,
    pub expires_at_ms: u64,
    pub epoch: u64,
}

#[derive(Default)]
pub(super) struct ReceiptLane {
    pub attach: Arc<tokio::sync::Mutex<()>>,
    pub exhausted: bool,
    pub grant: Option<ReceiptGrant>,
    pub epoch: u64,
    pub role: Option<String>,
    pub latest_turn: Option<String>,
    pub messages: Vec<String>,
    pub sink: Option<(CowdEventSender, u64)>,
}

impl std::fmt::Debug for ReceiptLane {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReceiptLane")
            .field("epoch", &self.epoch)
            .field("has_grant", &self.grant.is_some())
            .field("message_count", &self.messages.len())
            .finish()
    }
}

#[derive(Debug)]
pub(super) struct TerminalReceipts {
    slots: Arc<tokio::sync::Semaphore>,
    inflight: Mutex<std::collections::BTreeSet<(String, String)>>,
    #[cfg(test)]
    pub root: Mutex<Option<std::path::PathBuf>>,
    #[cfg(test)]
    pub save_gate: Mutex<Option<(Arc<tokio::sync::Notify>, std::sync::mpsc::Receiver<()>)>>,
    pub lanes: Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<ReceiptLane>>>>,
}

impl Default for TerminalReceipts {
    fn default() -> Self {
        Self {
            slots: Arc::new(tokio::sync::Semaphore::new(4)),
            inflight: Mutex::new(std::collections::BTreeSet::new()),
            lanes: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            root: Mutex::new(None),
            #[cfg(test)]
            save_gate: Mutex::new(None),
        }
    }
}

struct ReceiptFlight {
    owner: Arc<TerminalReceipts>,
    key: (String, String),
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for ReceiptFlight {
    fn drop(&mut self) {
        self.owner
            .inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}

impl TerminalReceipts {
    fn begin(
        self: &Arc<Self>,
        session_id: &str,
        message_id: &str,
    ) -> Result<ReceiptFlight, GatewayApiError> {
        let permit = self.slots.clone().try_acquire_owned().map_err(|_| {
            GatewayApiError::Contract("receipt save budget busy; retry from fresh context".into())
        })?;
        let key = (session_id.to_string(), message_id.to_string());
        if !self
            .inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.clone())
        {
            return Err(GatewayApiError::Contract(
                "exact receipt save already in flight".into(),
            ));
        }
        Ok(ReceiptFlight {
            owner: self.clone(),
            key,
            _permit: permit,
        })
    }

    pub fn lane(&self, session_id: &str) -> Arc<tokio::sync::Mutex<ReceiptLane>> {
        self.lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(session_id.into())
            .or_default()
            .clone()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(u64::MAX, |time| time.as_millis() as u64)
}

fn exact_context(
    value: &serde_json::Value,
    session_id: &str,
    message_id: &str,
    client_id: &str,
) -> Result<(), GatewayApiError> {
    let invalid = || GatewayApiError::Contract("invalid exact public receipt context".into());
    if value["status"] != "available"
        || value["session_id"] != session_id
        || value["message_id"] != message_id
        || value["client_id"] != client_id
        || value["payload_kind"] != "session-public-v1"
    {
        return Err(invalid());
    }
    for key in ["runtime_commit_cursor", "message_sequence"] {
        harness_contract::live::ReceiptCursor::parse(value[key].as_str().ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
    }
    let payload = value["payload_json"].as_str().ok_or_else(invalid)?;
    if payload.len() > 8 * 1024 * 1024
        || value["content_digest"]
            != format!(
                "session-public-v1:sha256:{:x}",
                Sha256::digest(payload.as_bytes())
            )
    {
        return Err(invalid());
    }
    Ok(())
}

fn private_directory(path: &std::path::Path) -> std::io::Result<()> {
    if !path.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "receipt root must be absolute",
        ));
    }
    let mut cursor = std::path::PathBuf::new();
    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "receipt root cannot traverse parents",
            ));
        }
        cursor.push(component);
        match std::fs::symlink_metadata(&cursor) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "receipt path contains a link or non-directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&cursor)?;
                std::fs::set_permissions(&cursor, std::fs::Permissions::from_mode(0o700))?;
                if let Some(parent) = cursor.parent() {
                    std::fs::File::open(parent)?.sync_all()?;
                }
            }
            Err(error) => return Err(error),
        }
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "receipt directory is not private",
        ));
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    std::fs::File::open(path)?.sync_all()
}

pub(super) fn persist_exact(
    value: &serde_json::Value,
    root: &std::path::Path,
) -> std::io::Result<()> {
    private_directory(root)?;
    let payload = value["payload_json"]
        .as_str()
        .ok_or_else(|| std::io::Error::other("payload missing"))?;
    let identity = serde_json::to_vec(&serde_json::json!({
        "session_id": value["session_id"], "message_id": value["message_id"], "client_id": value["client_id"],
        "message_sequence": value["message_sequence"], "runtime_commit_cursor": value["runtime_commit_cursor"],
        "content_digest": value["content_digest"], "payload_kind": value["payload_kind"]
    }))?;
    let key = format!("{:x}", Sha256::digest(&identity));
    let temporary = root.join(format!(".{key}.{}.pending", uuid::Uuid::new_v4()));
    let final_path = root.join(format!("{key}.public"));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temporary)?;
    let result = (|| {
        file.write_all(payload.as_bytes())?;
        file.flush()?;
        file.sync_all()?;
        std::fs::rename(&temporary, &final_path)?;
        std::fs::File::open(root)?.sync_all()?;
        let metadata_path = root.join(format!("{key}.identity"));
        let metadata_temporary =
            root.join(format!(".{key}.{}.identity.pending", uuid::Uuid::new_v4()));
        let mut metadata = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&metadata_temporary)?;
        metadata.write_all(&identity)?;
        metadata.flush()?;
        metadata.sync_all()?;
        std::fs::rename(&metadata_temporary, &metadata_path)?;
        std::fs::File::open(root)?.sync_all()
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

impl GatewayApiClient {
    #[cfg(test)]
    pub(super) async fn reject_old_receipt_for_test(
        &self,
        session_id: &str,
        message_id: &str,
        old: &serde_json::Value,
    ) -> bool {
        let grant = self
            .terminal_receipts
            .lane(session_id)
            .lock()
            .await
            .grant
            .clone()
            .unwrap();
        self.receipt_json(session_id,message_id,&grant,Some(serde_json::json!({
            "runtime_commit_cursor":old["runtime_commit_cursor"],"content_digest":old["content_digest"]
        }))).await.is_err()
    }
    pub(super) async fn invalidate_terminal_receipt_lane(&self, session_id: &str) {
        let lane = self.terminal_receipts.lane(session_id);
        let notification = {
            let mut state = lane.lock().await;
            let mut client_id = state
                .grant
                .take()
                .map(|grant| grant.client_id)
                .unwrap_or_default();
            if client_id.is_empty() && state.role.is_none() {
                return;
            }
            state.role = None;
            match state.epoch.checked_add(1) {
                Some(epoch) => state.epoch = epoch,
                None => {
                    state.exhausted = true;
                    client_id.clear();
                }
            }
            (state.sink.clone(), client_id, state.epoch)
        };
        if let (Some((tx, generation)), client_id, epoch) = notification {
            let _ = tx.send(session_scoped_event(
                session_id,
                generation,
                CowdEvent::TerminalReceiptSelection {
                    session_id: session_id.into(),
                    client_id,
                    epoch,
                },
            ));
        }
    }

    pub(crate) async fn expire_terminal_receipts(&self) {
        let lanes = self
            .terminal_receipts
            .lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(session, lane)| (session.clone(), lane.clone()))
            .collect::<Vec<_>>();
        for (session, lane) in lanes {
            let expired = lane
                .lock()
                .await
                .grant
                .as_ref()
                .is_some_and(|grant| now_ms() >= grant.expires_at_ms);
            if expired {
                self.invalidate_terminal_receipt_lane(&session).await;
            }
        }
    }

    async fn check_receipt_grant(
        &self,
        session_id: &str,
        grant: &ReceiptGrant,
        turn: Option<&str>,
    ) -> Result<(), GatewayApiError> {
        let lane = self.terminal_receipts.lane(session_id);
        let state = lane.lock().await;
        if state.exhausted
            || state.epoch != grant.epoch
            || now_ms() >= grant.expires_at_ms
            || state.grant.as_ref().is_none_or(|active| {
                active.client_id != grant.client_id
                    || active.epoch != grant.epoch
                    || now_ms() >= active.expires_at_ms
            })
            || turn.is_some_and(|turn| {
                state
                    .latest_turn
                    .as_deref()
                    .is_some_and(|current| current != turn)
            })
        {
            return Err(GatewayApiError::Contract("stale receipt grant".into()));
        }
        Ok(())
    }

    pub async fn refresh_terminal_receipts(&self, session_id: &str) {
        let sink = {
            let lane = self.terminal_receipts.lane(session_id);
            let state = lane.lock().await;
            state.sink.clone()
        };
        if let Some((tx, authority_generation)) = sink {
            self.publish_terminal_receipts(session_id, &tx, authority_generation)
                .await;
        }
    }

    async fn receipt_json(
        &self,
        session_id: &str,
        message_id: &str,
        grant: &ReceiptGrant,
        ack: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.check_receipt_grant(session_id, grant, None).await?;
        let suffix = if ack.is_some() {
            "ack"
        } else {
            "receipt-context"
        };
        let url = format!(
            "{}/api/sessions/{}/messages/{}/{}",
            self.base_url,
            url_encode(session_id),
            url_encode(message_id),
            suffix
        );
        let request = match ack {
            Some(body) => self.client.post(url).json(&body),
            None => self.client.get(url),
        };
        let mut response = self
            .authorize(request)
            .header("x-cowd-client-id", &grant.client_id)
            .header("cache-control", "no-store")
            .send()
            .await
            .map_err(GatewayApiError::Http)?;
        self.check_receipt_grant(session_id, grant, None).await?;
        if !response.status().is_success() {
            return Err(gateway_status_error(
                response.status(),
                response.text().await.unwrap_or_default(),
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(GatewayApiError::Http)? {
            self.check_receipt_grant(session_id, grant, None).await?;
            if bytes.len().saturating_add(chunk.len()) > 16 * 1024 * 1024 + 64 * 1024 {
                return Err(GatewayApiError::Contract(
                    "receipt response exceeds bounded wire budget".into(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        self.check_receipt_grant(session_id, grant, None).await?;
        serde_json::from_slice(&bytes).map_err(|error| GatewayApiError::Contract(error.to_string()))
    }

    pub(super) async fn recover_terminal_receipt(
        &self,
        session_id: &str,
        message_id: &str,
    ) -> Result<crate::protocol::TerminalReceiptOverlay, GatewayApiError> {
        let lane = self.terminal_receipts.lane(session_id);
        let epoch = lane.lock().await.epoch;
        let result = self
            .recover_terminal_receipt_inner(session_id, message_id)
            .await;
        if result.is_err() {
            let notification = {
                let mut state = lane.lock().await;
                if state.epoch != epoch {
                    None
                } else {
                    let client_id = state
                        .grant
                        .as_ref()
                        .map(|grant| grant.client_id.clone())
                        .unwrap_or_default();
                    match state.epoch.checked_add(1) {
                        Some(next) => {
                            state.epoch = next;
                            if let Some(grant) = state.grant.as_mut() {
                                grant.epoch = next;
                            }
                        }
                        None => {
                            state.exhausted = true;
                            state.grant = None;
                        }
                    }
                    Some((
                        state.sink.clone(),
                        if state.exhausted {
                            String::new()
                        } else {
                            client_id
                        },
                        state.epoch,
                    ))
                }
            };
            if let Some((Some((tx, generation)), client_id, epoch)) = notification {
                let _ = tx.send(session_scoped_event(
                    session_id,
                    generation,
                    CowdEvent::TerminalReceiptSelection {
                        session_id: session_id.into(),
                        client_id,
                        epoch,
                    },
                ));
            }
        }
        result
    }

    async fn recover_terminal_receipt_inner(
        &self,
        session_id: &str,
        message_id: &str,
    ) -> Result<crate::protocol::TerminalReceiptOverlay, GatewayApiError> {
        let _flight = self.terminal_receipts.begin(session_id, message_id)?;
        let lane = self.terminal_receipts.lane(session_id);
        let (grant, epoch) = {
            let mut state = lane.lock().await;
            let grant = state.grant.clone().ok_or_else(|| {
                GatewayApiError::Contract("receipt attachment unavailable".into())
            })?;
            if !state.messages.iter().any(|value| value == message_id) {
                if state.messages.len() >= 128 {
                    state.messages.remove(0);
                }
                state.messages.push(message_id.into());
            }
            (grant, state.epoch)
        };
        let context = self
            .receipt_json(session_id, message_id, &grant, None)
            .await?;
        exact_context(&context, session_id, message_id, &grant.client_id)?;
        let turn_id = context["session_source"]["turn_id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| GatewayApiError::Contract("receipt turn binding missing".into()))?
            .to_string();
        {
            let state = lane.lock().await;
            if state.epoch != epoch
                || state
                    .latest_turn
                    .as_ref()
                    .is_some_and(|current| current != &turn_id)
            {
                return Err(GatewayApiError::Contract(
                    "receipt is outside the selected turn epoch".into(),
                ));
            }
        }
        let root = std::env::var_os("XDG_STATE_HOME")
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(|home| std::path::PathBuf::from(home).join(".local/state"))
            })
            .ok_or_else(|| GatewayApiError::Contract("private state directory unavailable".into()))?
            .join("cowd/tui/terminal-receipts");
        #[cfg(test)]
        let root = self
            .terminal_receipts
            .root
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or(root);
        let save = context.clone();
        #[cfg(test)]
        let gate = self.terminal_receipts.save_gate.lock().unwrap().take();
        tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some((started, release)) = gate {
                started.notify_one();
                release.recv_timeout(Duration::from_secs(5)).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "late-save test barrier timed out",
                    )
                })?;
            }
            persist_exact(&save, &root)
        })
        .await
        .map_err(|error| GatewayApiError::Contract(error.to_string()))?
        .map_err(|error| GatewayApiError::Contract(error.to_string()))?;
        self.check_receipt_grant(session_id, &grant, Some(&turn_id))
            .await?;
        let current = self
            .receipt_json(session_id, message_id, &grant, None)
            .await?;
        exact_context(&current, session_id, message_id, &grant.client_id)?;
        for key in [
            "message_sequence",
            "runtime_commit_cursor",
            "content_digest",
            "payload_json",
        ] {
            if current[key] != context[key] {
                return Err(GatewayApiError::Contract(
                    "receipt changed during save".into(),
                ));
            }
        }
        let state = lane.lock().await;
        if state.epoch != epoch
            || state.grant.as_ref().is_none_or(|active| {
                active.client_id != grant.client_id || active.epoch != grant.epoch
            })
        {
            return Err(GatewayApiError::Contract(
                "stale receipt save completion".into(),
            ));
        }
        drop(state);
        self.check_receipt_grant(session_id, &grant, Some(&turn_id))
            .await?;
        self.receipt_json(session_id, message_id, &grant, Some(serde_json::json!({
            "runtime_commit_cursor": context["runtime_commit_cursor"], "content_digest": context["content_digest"]
        }))).await?;
        self.check_receipt_grant(session_id, &grant, Some(&turn_id))
            .await?;
        let fresh = self
            .receipt_json(session_id, message_id, &grant, None)
            .await?;
        exact_context(&fresh, session_id, message_id, &grant.client_id)?;
        self.check_receipt_grant(session_id, &grant, Some(&turn_id))
            .await?;
        let state = lane.lock().await;
        if state.epoch != epoch
            || state
                .latest_turn
                .as_ref()
                .is_some_and(|current| current != &turn_id)
        {
            return Err(GatewayApiError::Contract("stale receipt result".into()));
        }
        let received = fresh["receipt"].is_object()
            && [
                "session_id",
                "message_id",
                "client_id",
                "runtime_commit_cursor",
                "content_digest",
            ]
            .iter()
            .all(|key| fresh["receipt"][*key] == fresh[*key])
            && fresh["receipt"]["received_at_ms"].as_u64().is_some();
        Ok(crate::protocol::TerminalReceiptOverlay {
            session_id: session_id.into(),
            message_id: message_id.into(),
            client_id: grant.client_id,
            turn_id,
            epoch,
            received,
            expires_at_ms: grant.expires_at_ms,
            runtime_commit_cursor: fresh["runtime_commit_cursor"]
                .as_str()
                .unwrap_or_default()
                .into(),
            content_digest: fresh["content_digest"].as_str().unwrap_or_default().into(),
        })
    }

    pub(super) async fn publish_terminal_receipts(
        &self,
        session_id: &str,
        tx: &CowdEventSender,
        authority_generation: u64,
    ) {
        let messages = {
            let lane = self.terminal_receipts.lane(session_id);
            let mut state = lane.lock().await;
            state.sink = Some((tx.clone(), authority_generation));
            if let Some(grant) = state.grant.as_ref() {
                let _ = tx.send(session_scoped_event(
                    session_id,
                    authority_generation,
                    CowdEvent::TerminalReceiptSelection {
                        session_id: session_id.into(),
                        client_id: grant.client_id.clone(),
                        epoch: state.epoch,
                    },
                ));
            }
            state.messages.clone()
        };
        for message_id in messages {
            if let Ok(overlay) = self.recover_terminal_receipt(session_id, &message_id).await {
                let _ = tx.send(session_scoped_event(
                    session_id,
                    authority_generation,
                    CowdEvent::TerminalReceiptContext { overlay },
                ));
            }
        }
    }
}
