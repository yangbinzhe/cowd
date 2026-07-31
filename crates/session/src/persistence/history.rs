use super::{
    SessionListOptions, SessionMessage, SessionRecord, SessionSnapshot, UnifiedSessionStore,
};
use crate::domain::SessionDomainEventPage;
use crate::error::Result;

/// Read-only Session capability used by memory/context reconstruction.
///
/// This intentionally omits every mutation, outbox, claim, lease, branch, and
/// lifecycle operation. Consumers that only reconstruct context cannot become
/// a second Session owner by retaining the full repository.
#[derive(Debug, Clone)]
pub struct SessionHistoryReader {
    repository: UnifiedSessionStore,
}

impl SessionHistoryReader {
    pub(super) fn new(repository: UnifiedSessionStore) -> Self {
        Self { repository }
    }

    pub async fn list_recent_sessions(&self, limit: usize) -> Result<Vec<SessionRecord>> {
        Ok(self
            .repository
            .list_sessions_page(&SessionListOptions {
                query: None,
                model: None,
                status: None,
                sort: "last_activity",
                order: "desc",
                limit: limit.clamp(1, 500),
                offset: 0,
            })
            .await?
            .records)
    }

    /// Discover the current human/channel actor's own Session catalog.
    ///
    /// Workspace and principal are derived from the durable current Session,
    /// never from model-supplied input. This keeps active retrieval useful
    /// without turning an Agent into an unrestricted Session administrator.
    pub async fn discover_browsable_sessions(
        &self,
        current_session_id: &str,
        query: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<super::SessionListPage> {
        self.repository
            .discover_browsable_sessions(current_session_id, query, limit.clamp(1, 24), offset)
            .await
    }

    /// Verify whether one explicit target Session shares the current
    /// Session's durable workspace and actor identity.
    pub async fn can_read_session(
        &self,
        current_session_id: &str,
        target_session_id: &str,
    ) -> Result<bool> {
        if current_session_id == target_session_id {
            return Ok(true);
        }
        let Some(current) = self.repository.get_session(current_session_id).await? else {
            return Ok(false);
        };
        let Some(target) = self.repository.get_session(target_session_id).await? else {
            return Ok(false);
        };
        if matches!(target.status.as_str(), "deleted" | "deleting") {
            return Ok(false);
        }
        let current_metadata = session_metadata(&current);
        let target_metadata = session_metadata(&target);
        let current_workspace = metadata_text(&current_metadata, "workspace_root");
        let target_workspace = metadata_text(&target_metadata, "workspace_root");
        if current_workspace.is_none() || current_workspace != target_workspace {
            return Ok(false);
        }
        let current_owner = metadata_text(&current_metadata, "owner_principal_id");
        let target_owner = metadata_text(&target_metadata, "owner_principal_id");
        if current_owner.is_some() {
            return Ok(current_owner == target_owner);
        }
        Ok(current.user_id.as_deref().is_some_and(|user_id| {
            target.platform == current.platform && target.user_id.as_deref() == Some(user_id)
        }))
    }

    pub async fn session_record(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        self.repository.get_session(session_id).await
    }

    pub async fn message_count(&self, session_id: &str) -> Result<usize> {
        self.repository.get_message_count(session_id).await
    }

    pub async fn messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        self.repository
            .get_messages(session_id, offset, limit.clamp(1, 500))
            .await
    }

    /// Search message history inside exactly one Session.
    ///
    /// The caller supplies the authorized Session identity; this reader never
    /// broadens the query to another Session on its own.
    pub async fn search_messages(
        &self,
        query: &str,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        self.repository
            .search_messages(query, Some(session_id), limit.clamp(1, 100))
            .await
    }

    /// Search a pre-authorized Session set in one FTS query.
    ///
    /// Runtime obtains this set from its durable Session relation graph. The
    /// repository applies the scope in SQL before ranking so unrelated
    /// Sessions cannot displace authorized results.
    pub async fn search_messages_in_sessions(
        &self,
        query: &str,
        session_ids: &[String],
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.repository
            .search_messages_in_sessions(query, session_ids, limit.clamp(1, 100))
            .await
    }

    pub async fn latest_snapshot(&self, session_id: &str) -> Result<Option<SessionSnapshot>> {
        self.repository.get_latest_snapshot(session_id).await
    }

    pub async fn domain_events_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<SessionDomainEventPage> {
        self.repository
            .session_domain_events_page(session_id, from_sequence, limit.clamp(1, 4_096))
            .await
    }
}

fn session_metadata(record: &SessionRecord) -> serde_json::Value {
    record
        .metadata_json
        .as_deref()
        .and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or(serde_json::Value::Null)
}

fn metadata_text<'a>(metadata: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    metadata
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_record(id: &str) -> SessionRecord {
        SessionRecord {
            session_id: id.to_string(),
            platform: "test".to_string(),
            chat_id: id.to_string(),
            user_id: None,
            model: None,
            created_at: "2026-07-31T00:00:00Z".to_string(),
            last_activity: "2026-07-31T00:00:00Z".to_string(),
            message_count: 0,
            reset_policy: "None".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        }
    }

    fn message(session_id: &str, sequence: usize, text: &str) -> SessionMessage {
        SessionMessage {
            stable_message_id: format!("{session_id}-{sequence}"),
            session_id: session_id.to_string(),
            sequence,
            role: "user".to_string(),
            content_json: serde_json::json!([{"type":"text","text":text}]).to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: sequence as u64,
        }
    }

    #[tokio::test]
    async fn active_search_never_broadens_beyond_authorized_sessions() {
        let store = UnifiedSessionStore::open_in_memory().expect("session store");
        for session_id in ["current", "related", "unrelated"] {
            store
                .create_session(&session_record(session_id))
                .await
                .expect("session");
            store
                .insert_message(&message(
                    session_id,
                    0,
                    "shared retrieval marker for session history",
                ))
                .await
                .expect("message");
        }
        let reader = store.history_reader();

        let current = reader
            .search_messages("retrieval marker", "current", 10)
            .await
            .expect("current search");
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].session_id, "current");

        let related = reader
            .search_messages_in_sessions(
                "retrieval marker",
                &["current".to_string(), "related".to_string()],
                10,
            )
            .await
            .expect("related search");
        assert_eq!(related.len(), 2);
        assert!(related
            .iter()
            .all(|message| message.session_id != "unrelated"));
    }

    #[tokio::test]
    async fn session_catalog_and_explicit_reads_share_workspace_and_actor_identity() {
        let store = UnifiedSessionStore::open_in_memory().expect("session store");
        let records = [
            ("current", "/workspace/a", "human-a"),
            ("same-actor", "/workspace/a", "human-a"),
            ("other-actor", "/workspace/a", "human-b"),
            ("other-workspace", "/workspace/b", "human-a"),
        ];
        for (session_id, workspace, owner) in records {
            let mut record = session_record(session_id);
            record.platform = "webui".to_string();
            record.metadata_json = Some(
                serde_json::json!({
                    "title": format!("Architecture {session_id}"),
                    "workspace_root": workspace,
                    "owner_principal_id": owner,
                })
                .to_string(),
            );
            store.create_session(&record).await.expect("session");
            store
                .insert_message(&message(
                    session_id,
                    0,
                    "shared architecture marker in durable history",
                ))
                .await
                .expect("message");
        }
        let reader = store.history_reader();

        let page = reader
            .discover_browsable_sessions("current", Some("architecture marker"), 10, 0)
            .await
            .expect("discover own sessions");
        let ids = page
            .records
            .iter()
            .map(|record| record.session_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(page.total, 2);
        assert!(ids.contains(&"current"));
        assert!(ids.contains(&"same-actor"));
        assert!(!ids.contains(&"other-actor"));
        assert!(!ids.contains(&"other-workspace"));
        assert!(reader
            .can_read_session("current", "same-actor")
            .await
            .expect("same actor authorization"));
        assert!(!reader
            .can_read_session("current", "other-actor")
            .await
            .expect("other actor authorization"));
        assert!(!reader
            .can_read_session("current", "other-workspace")
            .await
            .expect("other workspace authorization"));
    }
}
