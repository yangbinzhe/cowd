use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{SessionPresenceProjection, UnifiedSessionStore};

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

#[derive(Debug, Clone, Default)]
struct SessionLifecycleEntry {
    state: SessionLifecycleState,
    attachments: HashMap<String, SessionAttachment>,
    next_sequence: usize,
    revision: u64,
    updated_at_ms: u64,
    persisted_at_ms: u64,
}

const DEFAULT_PRESENCE_TTL: Duration = Duration::from_secs(3_600);

#[derive(Debug)]
pub struct SessionPresenceLedger {
    sessions: RwLock<HashMap<String, SessionLifecycleEntry>>,
    store: Option<Arc<UnifiedSessionStore>>,
    mutation_locks: tokio::sync::Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    presence_ttl_ms: u64,
}

impl Default for SessionPresenceLedger {
    fn default() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            store: None,
            mutation_locks: tokio::sync::Mutex::new(HashMap::new()),
            presence_ttl_ms: duration_millis(DEFAULT_PRESENCE_TTL),
        }
    }
}

impl SessionPresenceLedger {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Runtime production construction. Presence remains hot mutable state;
    /// the store keeps only its latest restart checkpoint.
    #[must_use]
    pub fn with_store(store: Arc<UnifiedSessionStore>) -> Self {
        Self::with_store_and_ttl(store, DEFAULT_PRESENCE_TTL)
    }

    #[must_use]
    pub fn with_store_and_ttl(store: Arc<UnifiedSessionStore>, presence_ttl: Duration) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            store: Some(store),
            mutation_locks: tokio::sync::Mutex::new(HashMap::new()),
            presence_ttl_ms: duration_millis(presence_ttl),
        }
    }

    #[must_use]
    pub fn presence_ttl_ms(&self) -> u64 {
        self.presence_ttl_ms
    }

    pub async fn attach(
        &self,
        session_id: &str,
        actor: SessionActor,
    ) -> Result<SessionLifecycleEvent, String> {
        let session_id = validate_session_id(session_id)?;
        validate_actor(&actor)?;
        let mutation_lock = self.mutation_lock_for(&session_id).await;
        let _mutation = mutation_lock.lock().await;
        self.ensure_loaded_locked(&session_id).await?;

        let mut sessions = self.sessions.write().await;
        let previous = sessions.get(&session_id).cloned();
        let entry = sessions.entry(session_id.clone()).or_insert_with(|| {
            let now = current_epoch_ms();
            SessionLifecycleEntry {
                state: SessionLifecycleState::Created,
                attachments: HashMap::new(),
                next_sequence: 0,
                revision: 0,
                updated_at_ms: now,
                persisted_at_ms: 0,
            }
        });

        let now = current_epoch_ms();
        let expired = prune_expired_attachments(entry, now, self.presence_ttl_ms);
        if let Some(existing) = entry.attachments.get_mut(&actor.id) {
            if existing.actor.surface == actor.surface
                && existing.actor.role == actor.role
                && entry.state == SessionLifecycleState::Attached
            {
                existing.last_seen_ms = now;
                entry.updated_at_ms = now;
                let heartbeat_due = now.saturating_sub(entry.persisted_at_ms)
                    >= self.presence_heartbeat_persist_interval_ms();
                if expired || heartbeat_due {
                    entry.persisted_at_ms = now;
                }
                let snapshot =
                    (expired || heartbeat_due).then(|| snapshot_from_entry(&session_id, entry));
                let revision = if snapshot.is_some() {
                    entry.revision = entry.revision.saturating_add(1);
                    Some(entry.revision)
                } else {
                    None
                };
                let event = SessionLifecycleEvent {
                    session_id: session_id.clone(),
                    sequence: entry.next_sequence.saturating_sub(1),
                    event_type: "session.presence_observed".to_string(),
                    actor: Some(actor),
                    state: entry.state,
                    created_at_ms: now,
                };
                drop(sessions);
                if let (Some(snapshot), Some(revision)) = (snapshot, revision) {
                    if let Err(error) = self
                        .persist(
                            &snapshot,
                            previous.as_ref().map(|entry| entry.revision),
                            revision,
                        )
                        .await
                    {
                        self.restore_after_failed_persist(&session_id, previous, &error)
                            .await;
                        return Err(error);
                    }
                }
                return Ok(event);
            }
        }
        let attached_at_ms = entry
            .attachments
            .get(&actor.id)
            .map_or(now, |existing| existing.attached_at_ms);
        entry.attachments.insert(
            actor.id.clone(),
            SessionAttachment {
                session_id: session_id.clone(),
                actor: actor.clone(),
                attached_at_ms,
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
        entry.persisted_at_ms = now;
        let snapshot = snapshot_from_entry(&session_id, entry);
        entry.revision = entry.revision.saturating_add(1);
        let revision = entry.revision;
        drop(sessions);
        if let Err(error) = self
            .persist(
                &snapshot,
                previous.as_ref().map(|entry| entry.revision),
                revision,
            )
            .await
        {
            self.restore_after_failed_persist(&session_id, previous, &error)
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
        let mutation_lock = self.mutation_lock_for(&session_id).await;
        let _mutation = mutation_lock.lock().await;
        self.ensure_loaded_locked(&session_id).await?;

        let mut sessions = self.sessions.write().await;
        let previous = sessions.get(&session_id).cloned();
        let entry = sessions
            .entry(session_id.clone())
            .or_insert_with(SessionLifecycleEntry::default);
        let expired = prune_expired_attachments(entry, current_epoch_ms(), self.presence_ttl_ms);
        let actor = entry
            .attachments
            .remove(actor_id)
            .map(|attachment| attachment.actor);
        if actor.is_none() {
            let event = SessionLifecycleEvent {
                session_id: session_id.clone(),
                sequence: entry.next_sequence.saturating_sub(1),
                event_type: "session.presence_absent".to_string(),
                actor: None,
                state: entry.state,
                created_at_ms: current_epoch_ms(),
            };
            let snapshot = expired.then(|| snapshot_from_entry(&session_id, entry));
            let revision = if snapshot.is_some() {
                entry.persisted_at_ms = current_epoch_ms();
                entry.revision = entry.revision.saturating_add(1);
                Some(entry.revision)
            } else {
                None
            };
            drop(sessions);
            if let (Some(snapshot), Some(revision)) = (snapshot, revision) {
                if let Err(error) = self
                    .persist(
                        &snapshot,
                        previous.as_ref().map(|entry| entry.revision),
                        revision,
                    )
                    .await
                {
                    self.restore_after_failed_persist(&session_id, previous, &error)
                        .await;
                    return Err(error);
                }
            }
            return Ok(event);
        }
        let state = if entry.attachments.is_empty() {
            SessionLifecycleState::Detached
        } else {
            SessionLifecycleState::Attached
        };
        let now = current_epoch_ms();
        entry.state = state;
        entry.updated_at_ms = now;
        let event = Self::push_event(&session_id, entry, "session.detach", actor, state, now);
        entry.persisted_at_ms = now;
        let snapshot = snapshot_from_entry(&session_id, entry);
        entry.revision = entry.revision.saturating_add(1);
        let revision = entry.revision;
        drop(sessions);
        if let Err(error) = self
            .persist(
                &snapshot,
                previous.as_ref().map(|entry| entry.revision),
                revision,
            )
            .await
        {
            self.restore_after_failed_persist(&session_id, previous, &error)
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
        if self.expire_stale_attachments(session_id).await.is_err() {
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
        let mutation_lock = self.mutation_lock_for(&session_id).await;
        let _mutation = mutation_lock.lock().await;
        self.ensure_loaded_locked(&session_id).await?;
        let mut sessions = self.sessions.write().await;
        let previous = sessions.get(&session_id).cloned();
        let entry = sessions
            .entry(session_id.clone())
            .or_insert_with(SessionLifecycleEntry::default);
        let expired = prune_expired_attachments(entry, current_epoch_ms(), self.presence_ttl_ms);
        let now = current_epoch_ms();
        if entry.state == SessionLifecycleState::Active {
            entry.updated_at_ms = now;
            let event = SessionLifecycleEvent {
                session_id: session_id.clone(),
                sequence: entry.next_sequence.saturating_sub(1),
                event_type: "session.active_observed".to_string(),
                actor: None,
                state: entry.state,
                created_at_ms: now,
            };
            let snapshot = expired.then(|| snapshot_from_entry(&session_id, entry));
            let revision = if snapshot.is_some() {
                entry.persisted_at_ms = now;
                entry.revision = entry.revision.saturating_add(1);
                Some(entry.revision)
            } else {
                None
            };
            drop(sessions);
            if let (Some(snapshot), Some(revision)) = (snapshot, revision) {
                if let Err(error) = self
                    .persist(
                        &snapshot,
                        previous.as_ref().map(|entry| entry.revision),
                        revision,
                    )
                    .await
                {
                    self.restore_after_failed_persist(&session_id, previous, &error)
                        .await;
                    return Err(error);
                }
            }
            return Ok(event);
        }
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
        entry.persisted_at_ms = now;
        let snapshot = snapshot_from_entry(&session_id, entry);
        entry.revision = entry.revision.saturating_add(1);
        let revision = entry.revision;
        drop(sessions);
        if let Err(error) = self
            .persist(
                &snapshot,
                previous.as_ref().map(|entry| entry.revision),
                revision,
            )
            .await
        {
            self.restore_after_failed_persist(&session_id, previous, &error)
                .await;
            return Err(error);
        }
        Ok(event)
    }

    pub async fn snapshot(&self, session_id: &str) -> Option<SessionLifecycleSnapshot> {
        self.expire_stale_attachments(session_id).await.ok()?;
        let sessions = self.sessions.read().await;
        sessions
            .get(session_id)
            .map(|entry| snapshot_from_entry(session_id, entry))
    }

    pub async fn snapshots(&self) -> Vec<SessionLifecycleSnapshot> {
        if let Some(store) = &self.store {
            if let Ok(records) = store.list_sessions().await {
                for record in records {
                    let _ = self.expire_stale_attachments(&record.session_id).await;
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

    async fn mutation_lock_for(&self, session_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.mutation_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(session_id).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(session_id.to_string(), Arc::downgrade(&lock));
        lock
    }

    async fn ensure_loaded_locked(&self, session_id: &str) -> Result<(), String> {
        if self.sessions.read().await.contains_key(session_id) {
            return Ok(());
        }
        let Some(store) = &self.store else {
            return Ok(());
        };
        let Some(projection) = store
            .get_session_presence_projection(session_id)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(());
        };
        let state = serde_json::from_value::<SessionLifecycleState>(serde_json::Value::String(
            projection.state,
        ))
        .map_err(|error| format!("decode Session presence state: {error}"))?;
        let attachments =
            serde_json::from_str::<Vec<SessionAttachment>>(&projection.attachments_json)
                .map_err(|error| format!("decode Session presence attachments: {error}"))?;
        let entry = SessionLifecycleEntry {
            state,
            attachments: attachments
                .into_iter()
                .map(|attachment| (attachment.actor.id.clone(), attachment))
                .collect(),
            next_sequence: projection.next_sequence,
            revision: projection.revision,
            updated_at_ms: projection.updated_at_ms,
            persisted_at_ms: projection.updated_at_ms,
        };
        self.sessions
            .write()
            .await
            .entry(session_id.to_string())
            .or_insert(entry);
        Ok(())
    }

    async fn expire_stale_attachments(&self, session_id: &str) -> Result<(), String> {
        let session_id = validate_session_id(session_id)?;
        let mutation_lock = self.mutation_lock_for(&session_id).await;
        let _mutation = mutation_lock.lock().await;
        self.ensure_loaded_locked(&session_id).await?;

        let mut sessions = self.sessions.write().await;
        let previous = sessions.get(&session_id).cloned();
        let Some(entry) = sessions.get_mut(&session_id) else {
            return Ok(());
        };
        let now = current_epoch_ms();
        if !prune_expired_attachments(entry, now, self.presence_ttl_ms) {
            return Ok(());
        }
        entry.revision = entry.revision.saturating_add(1);
        let revision = entry.revision;
        entry.persisted_at_ms = now;
        let snapshot = snapshot_from_entry(&session_id, entry);
        drop(sessions);
        if let Err(error) = self
            .persist(
                &snapshot,
                previous.as_ref().map(|entry| entry.revision),
                revision,
            )
            .await
        {
            self.restore_after_failed_persist(&session_id, previous, &error)
                .await;
            return Err(error);
        }
        Ok(())
    }

    async fn persist(
        &self,
        snapshot: &SessionLifecycleSnapshot,
        expected_revision: Option<u64>,
        revision: u64,
    ) -> Result<(), String> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let attachments_json =
            serde_json::to_string(&snapshot.attachments).map_err(|error| error.to_string())?;
        let changed = store
            .compare_and_upsert_session_presence_projection(
                &SessionPresenceProjection {
                    session_id: snapshot.session_id.clone(),
                    state: snapshot.state.as_str().to_string(),
                    attachments_json,
                    next_sequence: snapshot.next_sequence,
                    revision,
                    updated_at_ms: snapshot.updated_at_ms,
                },
                expected_revision,
            )
            .await
            .map_err(|error| error.to_string())?;
        if !changed {
            return Err(format!(
                "Session presence revision conflict for `{}`; retry against the latest projection",
                snapshot.session_id
            ));
        }
        Ok(())
    }

    fn presence_heartbeat_persist_interval_ms(&self) -> u64 {
        (self.presence_ttl_ms / 3).max(1)
    }

    async fn restore_after_failed_persist(
        &self,
        session_id: &str,
        previous: Option<SessionLifecycleEntry>,
        error: &str,
    ) {
        let mut sessions = self.sessions.write().await;
        if error.contains("presence revision conflict") {
            sessions.remove(session_id);
            return;
        }
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

fn prune_expired_attachments(
    entry: &mut SessionLifecycleEntry,
    now_ms: u64,
    presence_ttl_ms: u64,
) -> bool {
    let before = entry.attachments.len();
    entry
        .attachments
        .retain(|_, attachment| now_ms.saturating_sub(attachment.last_seen_ms) < presence_ttl_ms);
    if entry.attachments.len() == before {
        return false;
    }
    if entry.attachments.is_empty() && entry.state == SessionLifecycleState::Attached {
        entry.state = SessionLifecycleState::Detached;
    }
    entry.updated_at_ms = now_ms;
    true
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
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

    async fn create_durable_session(store: &UnifiedSessionStore, session_id: &str) {
        let now = "2026-01-01T00:00:00Z".to_string();
        store
            .create_session(&crate::SessionRecord {
                session_id: session_id.to_string(),
                platform: "test".to_string(),
                chat_id: session_id.to_string(),
                user_id: None,
                model: None,
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                status: "active".to_string(),
            })
            .await
            .expect("durable Session");
    }

    #[tokio::test]
    async fn session_attach_does_not_create_duplicate_runtime() {
        let kernel = SessionPresenceLedger::new();
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
        assert_eq!(second.sequence, 0);
        assert_eq!(second.event_type, "session.presence_observed");
        assert_eq!(snapshot.attachments.len(), 1);
        assert_eq!(snapshot.state, SessionLifecycleState::Attached);
    }

    #[tokio::test]
    async fn tui_detach_keeps_session_active_for_other_surfaces() {
        let kernel = SessionPresenceLedger::new();
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
        let kernel = std::sync::Arc::new(SessionPresenceLedger::new());
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
    async fn lifecycle_mutations_are_serialized_per_session_not_globally() {
        let kernel = Arc::new(SessionPresenceLedger::new());
        let session_a_lock = kernel.mutation_lock_for("session-a").await;
        let session_a_guard = session_a_lock.lock().await;

        let other_session = {
            let kernel = Arc::clone(&kernel);
            tokio::spawn(async move { kernel.mark_active("session-b").await })
        };
        tokio::time::timeout(std::time::Duration::from_millis(250), other_session)
            .await
            .expect("session-b must not wait for session-a")
            .expect("session-b task")
            .expect("session-b mutation");

        let mut same_session = {
            let kernel = Arc::clone(&kernel);
            tokio::spawn(async move { kernel.mark_active("session-a").await })
        };
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), &mut same_session)
                .await
                .is_err(),
            "same-session mutation must wait for the current transaction"
        );
        drop(session_a_guard);
        same_session
            .await
            .expect("session-a task")
            .expect("session-a mutation");
    }

    #[tokio::test]
    async fn lifecycle_mutation_lock_registry_reclaims_completed_sessions() {
        let kernel = SessionPresenceLedger::new();
        {
            let lock = kernel.mutation_lock_for("session-a").await;
            let _guard = lock.lock().await;
            assert_eq!(kernel.mutation_locks.lock().await.len(), 1);
        }
        {
            let lock = kernel.mutation_lock_for("session-b").await;
            let _guard = lock.lock().await;
            let locks = kernel.mutation_locks.lock().await;
            assert_eq!(locks.len(), 1);
            assert!(locks.contains_key("session-b"));
        }
    }

    #[tokio::test]
    async fn reader_attach_can_replay_from_sequence_contract() {
        let kernel = SessionPresenceLedger::new();
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
            .create_session(&crate::SessionRecord {
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
                status: "active".to_string(),
            })
            .await
            .expect("session record");
        let first = SessionPresenceLedger::with_store(Arc::clone(&store));
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
        assert!(
            store
                .get_events("session-a", 0)
                .await
                .expect("events")
                .iter()
                .all(|event| event.event_type != "session.lifecycle.v1"),
            "presence must not amplify immutable Session history"
        );

        let rebuilt = SessionPresenceLedger::with_store(Arc::clone(&store));
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
    async fn presence_heartbeat_is_coalesced_but_refreshes_the_durable_lease() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().expect("store"));
        create_durable_session(&store, "session-heartbeat").await;
        let ledger = SessionPresenceLedger::with_store_and_ttl(
            Arc::clone(&store),
            Duration::from_millis(300),
        );
        ledger
            .attach("session-heartbeat", SessionActor::new("web-1", "webui"))
            .await
            .expect("initial attach");
        let first = store
            .get_session_presence_projection("session-heartbeat")
            .await
            .expect("first projection")
            .expect("first presence");

        ledger
            .attach("session-heartbeat", SessionActor::new("web-1", "webui"))
            .await
            .expect("coalesced heartbeat");
        let coalesced = store
            .get_session_presence_projection("session-heartbeat")
            .await
            .expect("coalesced projection")
            .expect("coalesced presence");
        assert_eq!(coalesced.revision, first.revision);

        tokio::time::sleep(Duration::from_millis(120)).await;
        ledger
            .attach("session-heartbeat", SessionActor::new("web-1", "webui"))
            .await
            .expect("durable heartbeat");
        let refreshed = store
            .get_session_presence_projection("session-heartbeat")
            .await
            .expect("refreshed projection")
            .expect("refreshed presence");
        assert_eq!(refreshed.revision, first.revision + 1);
        assert!(refreshed.updated_at_ms > first.updated_at_ms);
        assert!(
            store
                .get_events("session-heartbeat", 0)
                .await
                .expect("immutable events")
                .is_empty(),
            "coalesced presence heartbeat must remain outside immutable history"
        );
    }

    #[tokio::test]
    async fn stale_presence_writer_cannot_overwrite_a_newer_projection() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().expect("store"));
        create_durable_session(&store, "session-cas").await;
        let first = SessionPresenceLedger::with_store(Arc::clone(&store));
        first
            .attach("session-cas", SessionActor::new("web-1", "webui"))
            .await
            .expect("first attach");
        let stale = SessionPresenceLedger::with_store(Arc::clone(&store));
        stale.snapshot("session-cas").await.expect("stale snapshot");

        first
            .attach("session-cas", SessionActor::new("web-2", "webui"))
            .await
            .expect("newer attach");
        let error = stale
            .attach("session-cas", SessionActor::new("web-stale", "webui"))
            .await
            .expect_err("stale mutation must fail CAS");
        assert!(error.contains("revision conflict"));
        stale
            .attach("session-cas", SessionActor::new("web-stale", "webui"))
            .await
            .expect("retry reloads the latest projection");

        let rebuilt = SessionPresenceLedger::with_store(store);
        let snapshot = rebuilt
            .snapshot("session-cas")
            .await
            .expect("current state");
        let actors = snapshot
            .attachments
            .iter()
            .map(|attachment| attachment.actor.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            actors,
            std::collections::BTreeSet::from(["web-1", "web-2", "web-stale"])
        );
    }

    #[tokio::test]
    async fn stale_presence_expires_without_appending_session_history() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().expect("store"));
        let now = "2026-01-01T00:00:00Z".to_string();
        store
            .create_session(&crate::SessionRecord {
                session_id: "session-expired".to_string(),
                platform: "test".to_string(),
                chat_id: "session-expired".to_string(),
                user_id: None,
                model: None,
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                status: "active".to_string(),
            })
            .await
            .expect("session record");
        let stale_attachment = SessionAttachment {
            session_id: "session-expired".to_string(),
            actor: SessionActor::new("web-stale", "webui"),
            attached_at_ms: 1,
            last_seen_ms: 1,
        };
        store
            .upsert_session_presence_projection(&SessionPresenceProjection {
                session_id: "session-expired".to_string(),
                state: SessionLifecycleState::Attached.as_str().to_string(),
                attachments_json: serde_json::to_string(&vec![stale_attachment]).unwrap(),
                next_sequence: 1,
                revision: 1,
                updated_at_ms: 1,
            })
            .await
            .expect("stale projection");

        let ledger =
            SessionPresenceLedger::with_store_and_ttl(Arc::clone(&store), Duration::from_millis(1));
        let snapshot = ledger
            .snapshot("session-expired")
            .await
            .expect("expired snapshot remains queryable");

        assert!(snapshot.attachments.is_empty());
        assert_eq!(snapshot.state, SessionLifecycleState::Detached);
        let persisted = store
            .get_session_presence_projection("session-expired")
            .await
            .expect("presence query")
            .expect("presence projection");
        assert_eq!(persisted.attachments_json, "[]");
        assert!(
            !store
                .get_session_recovery_manifest("session-expired")
                .await
                .expect("recovery manifest")
                .expect("recovery row")
                .active_writer_or_attachment
        );
        assert!(
            store
                .get_events("session-expired", 0)
                .await
                .expect("events")
                .is_empty(),
            "presence expiry must not create immutable Session history"
        );
    }

    #[tokio::test]
    async fn failed_durable_append_restores_the_hot_cache() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().expect("store"));
        let kernel = SessionPresenceLedger::with_store(store);
        let error = kernel
            .attach("missing-session", SessionActor::new("web-1", "webui"))
            .await
            .expect_err("foreign session must not gain an in-memory lifecycle fact");
        assert!(!error.is_empty());
        assert!(kernel.snapshot("missing-session").await.is_none());
    }
}
