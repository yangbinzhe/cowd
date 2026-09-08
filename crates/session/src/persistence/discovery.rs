//! Resumable active discovery; all authority filtering remains in the store.
use super::{SessionHistoryReader, SessionMessage, SessionRecord, UnifiedSessionStore};
use crate::{SessionError, SessionResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionDiscoveryKind {
    Sessions,
    Messages,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionDiscoveryScope {
    Current,
    Explicit,
    Related,
    Workspace,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDiscoveryFilter {
    pub kind: SessionDiscoveryKind,
    pub scope: SessionDiscoveryScope,
    pub current_session_id: String,
    /// Runtime-authorized exact/related sessions. Workspace visibility is
    /// independently re-evaluated from durable actor/workspace rows in SQL.
    pub authorized_session_ids: Vec<String>,
    pub query: Option<String>,
    pub before_sequence: Option<usize>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionDiscoverySnapshot {
    pub fence: String,
    pub revisions: BTreeMap<String, i64>,
}
#[derive(Debug, Clone)]
pub struct SessionDiscoveryRequest {
    pub filter: SessionDiscoveryFilter,
    pub limit: usize,
    pub after_session_id: Option<String>,
    pub after_sequence: Option<usize>,
    pub snapshot: Option<SessionDiscoverySnapshot>,
}
#[derive(Debug, Clone)]
pub struct SessionDiscoveryPage {
    pub sessions: Vec<SessionRecord>,
    pub messages: Vec<SessionMessage>,
    pub snapshot: SessionDiscoverySnapshot,
    pub next_session_id: Option<String>,
    pub next_sequence: Option<usize>,
}
#[derive(Debug, Clone)]
pub struct SessionContextDiscoveryPage {
    pub sessions: Vec<SessionRecord>,
    pub messages: Vec<SessionMessage>,
    pub next_cursor: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    binding: String,
    after_session_id: String,
    after_sequence: Option<usize>,
    snapshot: SessionDiscoverySnapshot,
}

pub(super) async fn read_page(
    repository: &UnifiedSessionStore,
    mut filter: SessionDiscoveryFilter,
    cursor: Option<&str>,
    limit: usize,
) -> SessionResult<SessionContextDiscoveryPage> {
    if filter.current_session_id.trim().is_empty() {
        return Err(SessionError::InvalidArgument(
            "Session discovery requires a bound current Session".into(),
        ));
    }
    filter.authorized_session_ids.sort();
    filter.authorized_session_ids.dedup();
    filter.query = filter
        .query
        .map(|q| q.trim().to_lowercase())
        .filter(|q| !q.is_empty());
    if filter.kind == SessionDiscoveryKind::Sessions
        && (filter.scope != SessionDiscoveryScope::Workspace || filter.before_sequence.is_some())
    {
        return Err(SessionError::InvalidArgument(
            "Session catalog requires workspace scope and no message sequence".into(),
        ));
    }
    let binding = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&filter).map_err(|e| SessionError::Store(e.to_string()))?
        )
    );
    let cursor = cursor
        .map(|s| {
            serde_json::from_str::<Cursor>(s).map_err(|_| {
                SessionError::InvalidArgument("invalid Session discovery cursor".into())
            })
        })
        .transpose()?;
    if cursor.as_ref().is_some_and(|c| {
        c.version != 1
            || c.binding != binding
            || c.after_session_id.is_empty()
            || ((filter.kind == SessionDiscoveryKind::Messages) != c.after_sequence.is_some())
    }) {
        return Err(SessionError::InvalidArgument(
            "Session cursor does not match current scope, query or authority".into(),
        ));
    }
    let request = SessionDiscoveryRequest {
        filter,
        limit: limit.clamp(1, 100),
        after_session_id: cursor.as_ref().map(|c| c.after_session_id.clone()),
        after_sequence: cursor.as_ref().and_then(|c| c.after_sequence),
        snapshot: cursor.map(|c| c.snapshot),
    };
    let page = repository.discover_context_page(request).await?;
    let next_cursor = page
        .next_session_id
        .map(|after_session_id| {
            serde_json::to_string(&Cursor {
                version: 1,
                binding,
                after_session_id,
                after_sequence: page.next_sequence,
                snapshot: page.snapshot,
            })
            .map_err(|e| SessionError::Store(e.to_string()))
        })
        .transpose()?;
    Ok(SessionContextDiscoveryPage {
        sessions: page.sessions,
        messages: page.messages,
        next_cursor,
    })
}

impl SessionHistoryReader {
    /// Uses the same durable repository as history, without exposing writes.
    pub async fn discover_context(
        &self,
        filter: SessionDiscoveryFilter,
        cursor: Option<&str>,
        limit: usize,
    ) -> SessionResult<SessionContextDiscoveryPage> {
        read_page(self.discovery_repository(), filter, cursor, limit).await
    }
}
