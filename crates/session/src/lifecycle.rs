use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use memory::{SessionEvent, UnifiedSessionStore};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum SessionLifecycleState {
    #[default]
    Created,
    Active,
    Attached,
    Detached,
    Idle,
    Suspended,
    Restored,
    Closing,
    Closed,
    Failed,
}

impl SessionLifecycleState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Active => "active",
            Self::Attached => "attached",
            Self::Detached => "detached",
            Self::Idle => "idle",
            Self::Suspended => "suspended",
            Self::Restored => "restored",
            Self::Closing => "closing",
            Self::Closed => "closed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionActor {
    pub id: String,
    pub surface: String,
    #[serde(default)]
    pub role: Option<String>,
}

impl SessionActor {
    #[must_use]
    pub fn new(id: impl Into<String>, surface: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            surface: surface.into(),
            role: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionAttachment {
    pub session_id: String,
    pub actor: SessionActor,
    pub attached_at_ms: u64,
    pub last_seen_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLifecycleEvent {
    pub session_id: String,
    pub sequence: usize,
    pub event_type: String,
    pub actor: Option<SessionActor>,
    pub state: SessionLifecycleState,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLifecycleSnapshot {
    pub session_id: String,
    pub state: SessionLifecycleState,
    pub attachments: Vec<SessionAttachment>,
    pub next_sequence: usize,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DurableLifecycleEvent {
    event: SessionLifecycleEvent,
    snapshot: SessionLifecycleSnapshot,
}

#[derive(Debug, Clone, Default)]
struct SessionLifecycleEntry {
    state: SessionLifecycleState,
    attachments: HashMap<String, SessionAttachment>,
    next_sequence: usize,
    updated_at_ms: u64,
}

#[derive(Debug, Default)]
pub struct SessionLifecycleKernel {
    sessions: RwLock<HashMap<String, SessionLifecycleEntry>>,
    store: Option<Arc<UnifiedSessionStore>>,
    mutation_gate: tokio::sync::Mutex<()>,
}

impl SessionLifecycleKernel {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Runtime production construction. The kernel remains a hot cache, while
    /// lifecycle truth is appended to UnifiedSessionStore and reconstructed on
    /// demand after a Gateway restart.
    #[must_use]
    pub fn with_store(store: Arc<UnifiedSessionStore>) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            store: Some(store),
            mutation_gate: tokio::sync::Mutex::new(()),
        }
    }

    pub async fn attach(
        &self,
        session_id: &str,
        actor: SessionActor,
    ) -> Result<SessionLifecycleEvent, String> {
        let session_id = validate_session_id(session_id)?;
        validate_actor(&actor)?;
        let _mutation = self.mutation_gate.lock().await;
        self.ensure_loaded(&session_id).await?;

        let mut sessions = self.sessions.write().await;
        let previous = sessions.get(&session_id).cloned();
        let entry = sessions.entry(session_id.clone()).or_insert_with(|| {
            let now = current_epoch_ms();
            SessionLifecycleEntry {
                state: SessionLifecycleState::Created,
                attachments: HashMap::new(),
                next_sequence: 0,
                updated_at_ms: now,
            }
        });

        let now = current_epoch_ms();
        entry.attachments.insert(
            actor.id.clone(),
            SessionAttachment {
                session_id: session_id.clone(),
                actor: actor.clone(),
                attached_at_ms: now,
                last_seen_ms: now,
            },
        );
        entry.state = SessionLifecycleState::Attached;
        entry.updated_at_ms = now;
        let event = Self::push_event(
            &session_id,
            entry,
            "session.attach",
            Some(actor),
            SessionLifecycleState::Attached,
            now,
        );
        let snapshot = snapshot_from_entry(&session_id, entry);
        drop(sessions);
        if let Err(error) = self.persist(&event, &snapshot).await {
            self.restore_after_failed_persist(&session_id, previous)
                .await;
            return Err(error);
        }
        Ok(event)
    }

    pub async fn detach(
        &self,
        session_id: &str,
        actor_id: &str,
    ) -> Result<SessionLifecycleEvent, String> {
        let session_id = validate_session_id(session_id)?;
        if actor_id.trim().is_empty() {
            return Err("actor_id is required".to_string());
        }
        let _mutation = self.mutation_gate.lock().await;
        self.ensure_loaded(&session_id).await?;

        let mut sessions = self.sessions.write().await;
        let previous = sessions.get(&session_id).cloned();
        let entry = sessions
            .entry(session_id.clone())
            .or_insert_with(SessionLifecycleEntry::default);
        let actor = entry
            .attachments
            .remove(actor_id)
            .map(|attachment| attachment.actor);
        let state = if entry.attachments.is_empty() {
            SessionLifecycleState::Detached
        } else {
            SessionLifecycleState::Attached
        };
        let now = current_epoch_ms();
        entry.state = state;
        entry.updated_at_ms = now;
        let event = Self::push_event(&session_id, entry, "session.detach", actor, state, now);
        let snapshot = snapshot_from_entry(&session_id, entry);
        drop(sessions);
        if let Err(error) = self.persist(&event, &snapshot).await {
            self.restore_after_failed_persist(&session_id, previous)
                .await;
            return Err(error);
        }
        Ok(event)
    }

    /// Return the role of one concrete Surface attachment. Missing
    /// attachments remain distinguishable from explicit readers so stateless
    /// API clients can retain backwards-compatible admission while attached
    /// observer surfaces are enforced fail-closed.
    pub async fn attachment_role(&self, session_id: &str, actor_id: &str) -> Option<String> {
        if self.ensure_loaded(session_id).await.is_err() {
            return None;
        }
        self.sessions
            .read()
            .await
            .get(session_id)
            .and_then(|entry| entry.attachments.get(actor_id))
            .and_then(|attachment| attachment.actor.role.clone())
    }

    pub async fn mark_active(&self, session_id: &str) -> Result<SessionLifecycleEvent, String> {
        let session_id = validate_session_id(session_id)?;
        let _mutation = self.mutation_gate.lock().await;
        self.ensure_loaded(&session_id).await?;
        let mut sessions = self.sessions.write().await;
        let previous = sessions.get(&session_id).cloned();
        let entry = sessions
            .entry(session_id.clone())
            .or_insert_with(SessionLifecycleEntry::default);
        let now = current_epoch_ms();
        entry.state = SessionLifecycleState::Active;
        entry.updated_at_ms = now;
        let event = Self::push_event(
            &session_id,
            entry,
            "session.active",
            None,
            SessionLifecycleState::Active,
            now,
        );
        let snapshot = snapshot_from_entry(&session_id, entry);
        drop(sessions);
        if let Err(error) = self.persist(&event, &snapshot).await {
            self.restore_after_failed_persist(&session_id, previous)
                .await;
            return Err(error);
        }
        Ok(event)
    }

    pub async fn snapshot(&self, session_id: &str) -> Option<SessionLifecycleSnapshot> {
        self.ensure_loaded(session_id).await.ok()?;
        let sessions = self.sessions.read().await;
        sessions
            .get(session_id)
            .map(|entry| snapshot_from_entry(session_id, entry))
    }

    pub async fn snapshots(&self) -> Vec<SessionLifecycleSnapshot> {
        if let Some(store) = &self.store {
            if let Ok(records) = store.list_sessions().await {
                for record in records {
                    let _ = self.ensure_loaded(&record.session_id).await;
                }
            }
        }
        let sessions = self.sessions.read().await;
        let mut snapshots: Vec<_> = sessions
            .iter()
            .map(|(session_id, entry)| snapshot_from_entry(session_id, entry))
            .collect();
        snapshots.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        snapshots
    }

    fn push_event(
        session_id: &str,
        entry: &mut SessionLifecycleEntry,
        event_type: &str,
        actor: Option<SessionActor>,
        state: SessionLifecycleState,
        created_at_ms: u64,
    ) -> SessionLifecycleEvent {
        let sequence = entry.next_sequence;
        entry.next_sequence += 1;
        SessionLifecycleEvent {
            session_id: session_id.to_string(),
            sequence,
            event_type: event_type.to_string(),
            actor,
            state,
            created_at_ms,
        }
    }

    async fn ensure_loaded(&self, session_id: &str) -> Result<(), String> {
        if self.sessions.read().await.contains_key(session_id) {
            return Ok(());
        }
        let Some(store) = &self.store else {
            return Ok(());
        };
        let events = store
            .get_events(session_id, 0)
            .await
            .map_err(|error| error.to_string())?;
        let Some(snapshot) = events
            .iter()
            .filter(|event| event.event_type == "session.lifecycle.v1")
            .filter_map(|event| {
                serde_json::from_str::<DurableLifecycleEvent>(&event.event_json).ok()
            })
            .next_back()
            .map(|event| event.snapshot)
        else {
            return Ok(());
        };
        let entry = SessionLifecycleEntry {
            state: snapshot.state,
            attachments: snapshot
                .attachments
                .into_iter()
                .map(|attachment| (attachment.actor.id.clone(), attachment))
                .collect(),
            next_sequence: snapshot.next_sequence,
            updated_at_ms: snapshot.updated_at_ms,
        };
        self.sessions
            .write()
            .await
            .entry(session_id.to_string())
            .or_insert(entry);
        Ok(())
    }

    async fn persist(
        &self,
        event: &SessionLifecycleEvent,
        snapshot: &SessionLifecycleSnapshot,
    ) -> Result<(), String> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let event_json = serde_json::to_string(&DurableLifecycleEvent {
            event: event.clone(),
            snapshot: snapshot.clone(),
        })
        .map_err(|error| error.to_string())?;
        store
            .append_event_allocating_sequence(&SessionEvent {
                session_id: event.session_id.clone(),
                event_type: "session.lifecycle.v1".to_string(),
                event_json,
                sequence: 0,
                created_at_ms: event.created_at_ms,
            })
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    async fn restore_after_failed_persist(
        &self,
        session_id: &str,
        previous: Option<SessionLifecycleEntry>,
    ) {
        let mut sessions = self.sessions.write().await;
        match previous {
            Some(entry) => {
                sessions.insert(session_id.to_string(), entry);
            }
            None => {
                sessions.remove(session_id);
            }
        }
    }
}

fn snapshot_from_entry(
    session_id: &str,
    entry: &SessionLifecycleEntry,
) -> SessionLifecycleSnapshot {
    let mut attachments: Vec<_> = entry.attachments.values().cloned().collect();
    attachments.sort_by(|left, right| {
        left.actor
            .surface
            .cmp(&right.actor.surface)
            .then_with(|| left.actor.id.cmp(&right.actor.id))
    });
    SessionLifecycleSnapshot {
        session_id: session_id.to_string(),
        state: entry.state,
        attachments,
        next_sequence: entry.next_sequence,
        updated_at_ms: entry.updated_at_ms,
    }
}

fn validate_session_id(session_id: &str) -> Result<String, String> {
    let trimmed = session_id.trim();
    if trimmed.is_empty() {
        Err("session_id is required".to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

fn validate_actor(actor: &SessionActor) -> Result<(), String> {
    if actor.id.trim().is_empty() || actor.surface.trim().is_empty() {
        Err("actor.id and actor.surface are required".to_string())
    } else {
        Ok(())
    }
}

fn current_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn session_attach_does_not_create_duplicate_runtime() {
        let kernel = SessionLifecycleKernel::new();
        let first = kernel
            .attach("session-a", SessionActor::new("tui-1", "tui"))
            .await
            .expect("first attach succeeds");
        let second = kernel
            .attach("session-a", SessionActor::new("tui-1", "tui"))
            .await
            .expect("same actor attach refreshes attachment");

        let snapshot = kernel.snapshot("session-a").await.expect("snapshot exists");
        assert_eq!(first.sequence, 0);
        assert_eq!(second.sequence, 1);
        assert_eq!(snapshot.attachments.len(), 1);
        assert_eq!(snapshot.state, SessionLifecycleState::Attached);
    }

    #[tokio::test]
    async fn tui_detach_keeps_session_active_for_other_surfaces() {
        let kernel = SessionLifecycleKernel::new();
        kernel
            .attach("session-a", SessionActor::new("tui-1", "tui"))
            .await
            .unwrap();
        kernel
            .attach("session-a", SessionActor::new("web-1", "webui"))
            .await
            .unwrap();
        let detach = kernel.detach("session-a", "tui-1").await.unwrap();

        let snapshot = kernel.snapshot("session-a").await.unwrap();
        assert_eq!(detach.state, SessionLifecycleState::Attached);
        assert_eq!(snapshot.attachments.len(), 1);
        assert_eq!(snapshot.attachments[0].actor.id, "web-1");
    }

    #[tokio::test]
    async fn session_event_sequence_is_monotonic_under_concurrent_append() {
        let kernel = std::sync::Arc::new(SessionLifecycleKernel::new());
        let mut handles = Vec::new();
        for idx in 0..16 {
            let kernel = kernel.clone();
            handles.push(tokio::spawn(async move {
                kernel
                    .attach(
                        "session-concurrent",
                        SessionActor::new(format!("actor-{idx}"), "tui"),
                    )
                    .await
                    .expect("attach succeeds")
                    .sequence
            }));
        }

        let mut sequences = Vec::new();
        for handle in handles {
            sequences.push(handle.await.expect("join succeeds"));
        }
        sequences.sort_unstable();
        assert_eq!(sequences, (0..16).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn reader_attach_can_replay_from_sequence_contract() {
        let kernel = SessionLifecycleKernel::new();
        kernel
            .attach("session-a", SessionActor::new("tui-1", "tui"))
            .await
            .unwrap();
        kernel.mark_active("session-a").await.unwrap();

        let snapshot = kernel.snapshot("session-a").await.unwrap();
        assert_eq!(snapshot.next_sequence, 2);
    }

    #[tokio::test]
    async fn durable_kernel_rebuilds_attachment_state_after_restart() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().expect("store"));
        let now = "2026-01-01T00:00:00Z".to_string();
        store
            .create_session(&memory::SessionRecord {
                session_id: "session-a".to_string(),
                platform: "test".to_string(),
                chat_id: "session-a".to_string(),
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
            .expect("session record");
        let first = SessionLifecycleKernel::with_store(Arc::clone(&store));
        first
            .attach("session-a", SessionActor::new("web-1", "webui"))
            .await
            .expect("attach");
        assert!(
            store
                .get_session_recovery_manifest("session-a")
                .await
                .expect("recovery manifest")
                .expect("recovery row")
                .active_writer_or_attachment
        );
        first.mark_active("session-a").await.expect("active");

        let rebuilt = SessionLifecycleKernel::with_store(Arc::clone(&store));
        let snapshot = rebuilt.snapshot("session-a").await.expect("rehydrated");
        assert_eq!(snapshot.state, SessionLifecycleState::Active);
        assert_eq!(snapshot.attachments.len(), 1);
        assert_eq!(snapshot.attachments[0].actor.id, "web-1");
        assert_eq!(snapshot.next_sequence, 2);

        rebuilt.detach("session-a", "web-1").await.expect("detach");
        assert!(
            !store
                .get_session_recovery_manifest("session-a")
                .await
                .expect("recovery manifest")
                .expect("recovery row")
                .active_writer_or_attachment
        );
    }

    #[tokio::test]
    async fn failed_durable_append_restores_the_hot_cache() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().expect("store"));
        let kernel = SessionLifecycleKernel::with_store(store);
        let error = kernel
            .attach("missing-session", SessionActor::new("web-1", "webui"))
            .await
            .expect_err("foreign session must not gain an in-memory lifecycle fact");
        assert!(!error.is_empty());
        assert!(kernel.snapshot("missing-session").await.is_none());
    }
}
