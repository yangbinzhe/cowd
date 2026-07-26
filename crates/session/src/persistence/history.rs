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
