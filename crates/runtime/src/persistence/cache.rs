use std::sync::Mutex;
use async_trait::async_trait;
use lru::LruCache;
use crate::persistence::{PersistenceProtocol, Result, StoreStats};
use crate::session::{ConversationMessage, SessionRecord};

struct CachedSession {
    messages: Vec<ConversationMessage>,
    message_count: usize,
    last_sequence: usize,
}

pub struct CachedPersistence<P: PersistenceProtocol> {
    inner: P,
    cache: Mutex<LruCache<String, CachedSession>>,
}

impl<P: PersistenceProtocol> CachedPersistence<P> {
    pub fn new(inner: P, max_entries: usize) -> Self {
        Self {
            inner,
            cache: Mutex::new(LruCache::new(std::num::NonZeroUsize::new(max_entries.max(1)).unwrap())),
        }
    }
}

#[async_trait]
impl<P: PersistenceProtocol + Send + Sync> PersistenceProtocol for CachedPersistence<P> {
    async fn create_session(&self, record: &SessionRecord) -> Result<()> { self.inner.create_session(record).await }
    async fn get_session(&self, id: &str) -> Result<Option<SessionRecord>> { self.inner.get_session(id).await }
    async fn list_sessions(&self) -> Result<Vec<SessionRecord>> { self.inner.list_sessions().await }
    async fn update_session(&self, id: &str, record: &SessionRecord) -> Result<()> { self.inner.update_session(id, record).await }
    async fn delete_session(&self, id: &str) -> Result<()> { self.inner.delete_session(id).await }

    async fn append_message(&self, session_id: &str, msg: &ConversationMessage) -> Result<()> {
        self.inner.append_message(session_id, msg).await?;
        if let Ok(mut cache) = self.cache.lock() {
            if let Some(cached) = cache.get_mut(session_id) {
                cached.messages.push(msg.clone());
                cached.message_count += 1;
                cached.last_sequence += 1;
            }
        }
        Ok(())
    }

    async fn append_messages(&self, session_id: &str, msgs: &[ConversationMessage]) -> Result<()> {
        self.inner.append_messages(session_id, msgs).await
    }

    async fn get_messages(&self, session_id: &str) -> Result<Vec<ConversationMessage>> {
        {
            let cache = self.cache.lock().unwrap();
            if let Some(cached) = cache.peek(session_id) {
                return Ok(cached.messages.clone());
            }
        }
        let messages = self.inner.get_messages(session_id).await?;
        let mut cache = self.cache.lock().unwrap();
        cache.put(session_id.to_string(), CachedSession {
            message_count: messages.len(),
            last_sequence: messages.len().saturating_sub(1),
            messages: messages.clone(),
        });
        Ok(messages)
    }

    async fn get_messages_range(&self, session_id: &str, from: usize, limit: usize) -> Result<Vec<ConversationMessage>> {
        self.inner.get_messages_range(session_id, from, limit).await
    }
    async fn get_message_count(&self, session_id: &str) -> Result<usize> { self.inner.get_message_count(session_id).await }
    async fn delete_messages_from(&self, session_id: &str, sequence: usize) -> Result<()> {
        self.inner.delete_messages_from(session_id, sequence).await
    }
    async fn search_messages(&self, query: &str) -> Result<Vec<ConversationMessage>> { self.inner.search_messages(query).await }
    async fn search_sessions(&self, query: &str) -> Result<Vec<SessionRecord>> { self.inner.search_sessions(query).await }
    async fn save_snapshot(&self, session_id: &str, messages: &[ConversationMessage]) -> Result<()> { self.inner.save_snapshot(session_id, messages).await }
    async fn get_latest_snapshot(&self, session_id: &str) -> Result<Option<Vec<ConversationMessage>>> { self.inner.get_latest_snapshot(session_id).await }
    async fn cleanup(&self) -> Result<usize> {
        let deleted = self.inner.cleanup().await?;
        if deleted > 0 { self.cache.lock().unwrap().clear(); }
        Ok(deleted)
    }
    async fn flush(&self) -> Result<()> { self.inner.flush().await }
    async fn stats(&self) -> Result<StoreStats> { self.inner.stats().await }
}
