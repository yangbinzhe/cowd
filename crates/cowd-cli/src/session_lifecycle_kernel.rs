use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SessionLifecycleState {
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
    pub(crate) fn as_str(self) -> &'static str {
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
pub(crate) struct SessionActor {
    pub(crate) id: String,
    pub(crate) surface: String,
    #[serde(default)]
    pub(crate) role: Option<String>,
}

impl SessionActor {
    #[must_use]
    pub(crate) fn new(id: impl Into<String>, surface: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            surface: surface.into(),
            role: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SessionAttachment {
    pub(crate) session_id: String,
    pub(crate) actor: SessionActor,
    pub(crate) attached_at_ms: u64,
    pub(crate) last_seen_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SessionLifecycleEvent {
    pub(crate) session_id: String,
    pub(crate) sequence: usize,
    pub(crate) event_type: String,
    pub(crate) actor: Option<SessionActor>,
    pub(crate) state: SessionLifecycleState,
    pub(crate) created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SessionLifecycleSnapshot {
    pub(crate) session_id: String,
    pub(crate) state: SessionLifecycleState,
    pub(crate) attachments: Vec<SessionAttachment>,
    pub(crate) next_sequence: usize,
    pub(crate) updated_at_ms: u64,
}

#[derive(Debug, Default)]
struct SessionLifecycleEntry {
    state: SessionLifecycleState,
    attachments: HashMap<String, SessionAttachment>,
    next_sequence: usize,
    updated_at_ms: u64,
}

impl Default for SessionLifecycleState {
    fn default() -> Self {
        Self::Created
    }
}

#[derive(Debug, Default)]
pub(crate) struct SessionLifecycleKernel {
    sessions: RwLock<HashMap<String, SessionLifecycleEntry>>,
}

impl SessionLifecycleKernel {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn attach(
        &self,
        session_id: &str,
        actor: SessionActor,
    ) -> Result<SessionLifecycleEvent, String> {
        let session_id = validate_session_id(session_id)?;
        validate_actor(&actor)?;

        let mut sessions = self.sessions.write().await;
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
        Ok(Self::push_event(
            &session_id,
            entry,
            "session.attach",
            Some(actor),
            SessionLifecycleState::Attached,
            now,
        ))
    }

    pub(crate) async fn detach(
        &self,
        session_id: &str,
        actor_id: &str,
    ) -> Result<SessionLifecycleEvent, String> {
        let session_id = validate_session_id(session_id)?;
        if actor_id.trim().is_empty() {
            return Err("actor_id is required".to_string());
        }

        let mut sessions = self.sessions.write().await;
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
        Ok(Self::push_event(
            &session_id,
            entry,
            "session.detach",
            actor,
            state,
            now,
        ))
    }

    pub(crate) async fn mark_active(
        &self,
        session_id: &str,
    ) -> Result<SessionLifecycleEvent, String> {
        let session_id = validate_session_id(session_id)?;
        let mut sessions = self.sessions.write().await;
        let entry = sessions
            .entry(session_id.clone())
            .or_insert_with(SessionLifecycleEntry::default);
        let now = current_epoch_ms();
        entry.state = SessionLifecycleState::Active;
        entry.updated_at_ms = now;
        Ok(Self::push_event(
            &session_id,
            entry,
            "session.active",
            None,
            SessionLifecycleState::Active,
            now,
        ))
    }

    pub(crate) async fn snapshot(&self, session_id: &str) -> Option<SessionLifecycleSnapshot> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).map(|entry| {
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
        })
    }

    pub(crate) async fn snapshots(&self) -> Vec<SessionLifecycleSnapshot> {
        let sessions = self.sessions.read().await;
        let mut snapshots: Vec<_> = sessions
            .iter()
            .map(|(session_id, entry)| {
                let mut attachments: Vec<_> = entry.attachments.values().cloned().collect();
                attachments.sort_by(|left, right| {
                    left.actor
                        .surface
                        .cmp(&right.actor.surface)
                        .then_with(|| left.actor.id.cmp(&right.actor.id))
                });
                SessionLifecycleSnapshot {
                    session_id: session_id.clone(),
                    state: entry.state,
                    attachments,
                    next_sequence: entry.next_sequence,
                    updated_at_ms: entry.updated_at_ms,
                }
            })
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
}
