use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

pub type SessionId = String;

pub struct SessionConfig {
    pub model: String,
    pub permission_mode: crate::permissions::PermissionMode,
    pub system_prompt: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub id: SessionId,
    pub model: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_activity: chrono::DateTime<chrono::Utc>,
    pub message_count: usize,
    pub token_count: u64,
    pub status: SessionStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SessionStatus { Active, Suspended, Archived }

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("Session not found: {0}")]
    NotFound(SessionId),
    #[error("Session creation failed: {0}")]
    CreationFailed(String),
}

pub trait SessionManager: Send + Sync {
    fn create(&self, config: SessionConfig) -> Result<SessionId, SessionError>;
    fn get(&self, id: &SessionId) -> Option<Arc<()>>; // placeholder, real impl returns ConversationRuntime
    fn list(&self) -> Vec<SessionInfo>;
    fn info(&self, id: &SessionId) -> Option<SessionInfo>;
    fn suspend(&self, id: &SessionId) -> Result<(), SessionError>;
    fn resume(&self, id: &SessionId) -> Option<Arc<()>>;
    fn archive(&self, id: &SessionId) -> Result<(), SessionError>;
    fn delete(&self, id: &SessionId) -> Result<(), SessionError>;
}

pub struct InMemorySessionManager {
    sessions: RwLock<HashMap<SessionId, SessionEntry>>,
}

struct SessionEntry {
    config: SessionConfig,
    info: SessionInfo,
    created_at: std::time::Instant,
}

impl InMemorySessionManager {
    pub fn new() -> Self {
        Self { sessions: RwLock::new(HashMap::new()) }
    }
}

impl SessionManager for InMemorySessionManager {
    fn create(&self, config: SessionConfig) -> Result<SessionId, SessionError> {
        let id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now();
        let model = config.model.clone();
        let entry = SessionEntry {
            config,
            info: SessionInfo {
                id: id.clone(),
                model,
                created_at: now,
                last_activity: now,
                message_count: 0,
                token_count: 0,
                status: SessionStatus::Active,
            },
            created_at: std::time::Instant::now(),
        };
        self.sessions.write().map_err(|_| SessionError::CreationFailed("lock poisoned".into()))?
            .insert(id.clone(), entry);
        Ok(id)
    }

    fn get(&self, id: &SessionId) -> Option<Arc<()>> {
        let _ = self.sessions.read().ok()?.get(id)?;
        None // placeholder - real impl returns Arc<ConversationRuntime>
    }

    fn list(&self) -> Vec<SessionInfo> {
        self.sessions.read().ok()
            .map(|s| s.values().map(|e| e.info.clone()).collect())
            .unwrap_or_default()
    }

    fn info(&self, id: &SessionId) -> Option<SessionInfo> {
        self.sessions.read().ok()?.get(id).map(|e| e.info.clone())
    }

    fn suspend(&self, id: &SessionId) -> Result<(), SessionError> {
        self.sessions.write().map_err(|_| SessionError::CreationFailed("lock poisoned".into()))?
            .get_mut(id).map(|e| e.info.status = SessionStatus::Suspended)
            .ok_or_else(|| SessionError::NotFound(id.clone()))
    }

    fn resume(&self, id: &SessionId) -> Option<Arc<()>> {
        self.sessions.write().ok()?.get_mut(id).map(|e| e.info.status = SessionStatus::Active);
        None
    }

    fn archive(&self, id: &SessionId) -> Result<(), SessionError> {
        self.sessions.write().map_err(|_| SessionError::CreationFailed("lock poisoned".into()))?
            .get_mut(id).map(|e| e.info.status = SessionStatus::Archived)
            .ok_or_else(|| SessionError::NotFound(id.clone()))
    }

    fn delete(&self, id: &SessionId) -> Result<(), SessionError> {
        self.sessions.write().map_err(|_| SessionError::CreationFailed("lock poisoned".into()))?
            .remove(id)
            .map(|_| ())
            .ok_or_else(|| SessionError::NotFound(id.clone()))
    }
}
