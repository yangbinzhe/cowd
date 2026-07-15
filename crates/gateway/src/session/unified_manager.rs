use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use futures::{stream, StreamExt};
use memory::{SessionMissionOutboxOperation, SessionMissionOutboxRequest, SessionRecord};
use runtime::session_lifecycle::SessionLifecycleManager;
use tokio::sync::Mutex;

use crate::runtime_service::RuntimeService;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionSource {
    WebUi,
    Tui,
    Surface(String),
    MissionControl,
    Socket,
    Cli,
    Internal,
}

impl SessionSource {
    fn platform(&self) -> &str {
        match self {
            Self::WebUi => "webui",
            Self::Tui => "tui",
            Self::Surface(surface) => surface,
            Self::MissionControl => "mission_control",
            Self::Socket => "socket",
            Self::Cli => "cli",
            Self::Internal => "internal",
        }
    }

    fn system_prompt(&self) -> Vec<String> {
        let Self::Surface(surface) = self else {
            return Vec::new();
        };
        vec![format!(
            "你正在通过 `{surface}` 外部 surface 回复用户。必须优先给出可见、简洁、可执行的阶段性结果。\
            如果任务需要读代码、检查 README、调研或测试，只检查足以支撑结论的关键证据；不要进行无边界穷举。\
            如果当前 turn 的信息或时间不足，直接说明已检查内容、当前判断、剩余风险和建议下一步，而不是持续调用工具直到超时。\
            外部 surface 的用户体验要求：宁可给出有证据的阶段性结论，也不能让用户长时间没有任何回复。"
        )]
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EnsureSessionRequest {
    pub(crate) session_id: String,
    pub(crate) model: Option<String>,
    pub(crate) source: SessionSource,
    pub(crate) title: Option<String>,
    pub(crate) user_id: Option<String>,
    pub(crate) chat_id: Option<String>,
    pub(crate) metadata: serde_json::Value,
    pub(crate) mission_operation: SessionMissionOutboxOperation,
}

impl EnsureSessionRequest {
    pub(crate) fn new(
        session_id: impl Into<String>,
        model: Option<String>,
        source: SessionSource,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            model,
            source,
            title: None,
            user_id: None,
            chat_id: None,
            metadata: serde_json::json!({}),
            mission_operation: SessionMissionOutboxOperation::Register,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EnsureSessionOutcome {
    pub(crate) session_id: String,
    pub(crate) model: String,
    pub(crate) created: bool,
    pub(crate) restored: bool,
    pub(crate) record: SessionRecord,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct SessionRecoverySummary {
    pub(crate) discovered: usize,
    pub(crate) recovered: usize,
    pub(crate) already_active: usize,
    pub(crate) failed: usize,
    pub(crate) failures: Vec<String>,
}

/// Exclusive coordinator for durable Session identity and process-local Runtime state.
///
/// RuntimeService remains the low-level Runtime factory/cache. SessionKernel remains
/// the durable store boundary. Callers must use this manager so those resources and
/// both lifecycle projections change as one operation.
pub(crate) struct UnifiedSessionManager {
    runtime: Arc<RuntimeService>,
    resource_lifecycle: Arc<SessionLifecycleManager>,
    session_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    max_active_sessions: usize,
}

impl UnifiedSessionManager {
    #[must_use]
    pub(crate) fn new(
        runtime: Arc<RuntimeService>,
        resource_lifecycle: Arc<SessionLifecycleManager>,
        max_active_sessions: usize,
    ) -> Self {
        Self {
            runtime,
            resource_lifecycle,
            session_locks: Mutex::new(HashMap::new()),
            max_active_sessions: max_active_sessions.max(1),
        }
    }

    async fn lock_for(&self, session_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.session_locks.lock().await;
        locks
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(crate) async fn ensure_session(
        &self,
        request: EnsureSessionRequest,
    ) -> Result<EnsureSessionOutcome, String> {
        let session_id = request.session_id.trim();
        if session_id.is_empty() {
            return Err("session id is required".to_string());
        }
        let lock = self.lock_for(session_id).await;
        let _guard = lock.lock().await;
        let session_kernel = self.runtime.session_kernel();
        let existing = session_kernel
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?;

        if self.runtime.has_active_session(session_id) {
            self.resource_lifecycle.register(session_id).await;
            self.resource_lifecycle.mark_active(session_id).await;
            let record = match existing {
                Some(record) => record,
                None => {
                    let record = self.persist_new_record(&request).await?;
                    if let Err(error) = self
                        .runtime
                        .lifecycle_kernel()
                        .mark_active(session_id)
                        .await
                    {
                        return Err(self
                            .rollback_created_session(&record, error.to_string())
                            .await);
                    }
                    record
                }
            };
            return Ok(EnsureSessionOutcome {
                session_id: session_id.to_string(),
                model: record
                    .model
                    .clone()
                    .unwrap_or_else(|| crate::DEFAULT_MODEL.to_string()),
                created: false,
                restored: false,
                record,
            });
        }

        if existing.is_none()
            && session_kernel.list_active_session_ids().len() >= self.max_active_sessions
        {
            return Err(format!(
                "active session limit {} reached",
                self.max_active_sessions
            ));
        }

        let created = existing.is_none();
        let record = match existing {
            Some(record) => record,
            None => self.persist_new_record(&request).await?,
        };
        if !created
            && self
                .runtime
                .session_kernel()
                .list_active_session_ids()
                .len()
                >= self.max_active_sessions
        {
            let victim = self
                .resource_lifecycle
                .evict_one_for_capacity()
                .await
                .ok_or_else(|| {
                    "active Session registry is full but lifecycle has no eviction candidate"
                        .to_string()
                })?;
            self.runtime.remove_active_runtime_if_present(&victim);
        }
        let model = request
            .model
            .as_deref()
            .filter(|model| !model.trim().is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| {
                record
                    .model
                    .clone()
                    .filter(|model| !model.trim().is_empty())
            })
            .unwrap_or_else(|| crate::DEFAULT_MODEL.to_string());

        if let Err(error) = self
            .runtime
            .activate_persisted_session(session_id, Some(&model), request.source.system_prompt())
            .await
        {
            if created {
                return Err(self.rollback_created_session(&record, error).await);
            }
            return Err(error);
        }
        if let Err(error) = self.register_lifecycle(session_id, !created).await {
            self.runtime.remove_active_runtime_if_present(session_id);
            self.resource_lifecycle.unregister(session_id).await;
            if created {
                return Err(self.rollback_created_session(&record, error).await);
            }
            return Err(error);
        }
        Ok(EnsureSessionOutcome {
            session_id: session_id.to_string(),
            model,
            created,
            restored: !created,
            record,
        })
    }

    async fn persist_new_record(
        &self,
        request: &EnsureSessionRequest,
    ) -> Result<SessionRecord, String> {
        let now = Utc::now().to_rfc3339();
        let platform = request.source.platform().to_string();
        let title = request.title.clone().unwrap_or_else(|| {
            format!(
                "{} {}",
                platform,
                request.session_id.chars().take(8).collect::<String>()
            )
        });
        let mut metadata = request.metadata.as_object().cloned().unwrap_or_default();
        metadata.insert(
            "title".to_string(),
            serde_json::Value::String(title.clone()),
        );
        metadata.insert(
            "source".to_string(),
            serde_json::Value::String(platform.clone()),
        );
        metadata.insert(
            "workspace_root".to_string(),
            serde_json::Value::String(
                self.runtime
                    .runtime_services()
                    .workspace_root()
                    .display()
                    .to_string(),
            ),
        );
        let record = SessionRecord {
            session_id: request.session_id.clone(),
            platform,
            chat_id: request
                .chat_id
                .clone()
                .unwrap_or_else(|| request.session_id.clone()),
            user_id: request.user_id.clone(),
            model: request
                .model
                .clone()
                .filter(|model| !model.trim().is_empty())
                .or_else(|| Some(crate::DEFAULT_MODEL.to_string())),
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: Some(serde_json::Value::Object(metadata).to_string()),
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        let workspace_key = self.runtime.runtime_services().workspace_key().to_string();
        let operation_name = match request.mission_operation {
            SessionMissionOutboxOperation::Register => "register",
            SessionMissionOutboxOperation::Start => "start",
            SessionMissionOutboxOperation::Close => "close",
        };
        let outbox = SessionMissionOutboxRequest {
            request_id: format!(
                "mission:{workspace_key}:{operation_name}:{}:{}",
                record.session_id, record.created_at
            ),
            session_id: record.session_id.clone(),
            title,
            workspace_key,
            operation: request.mission_operation,
            created_at_ms: current_time_ms(),
        };
        self.runtime
            .session_kernel()
            .upsert_stored_session_with_mission_outbox(&record, &outbox)
            .await
            .map_err(|error| error.to_string())?;
        Ok(record)
    }

    fn close_outbox_request(&self, record: &SessionRecord) -> SessionMissionOutboxRequest {
        let title = record
            .metadata_json
            .as_deref()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .and_then(|value| {
                value
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_else(|| {
                format!(
                    "Session {}",
                    record.session_id.chars().take(8).collect::<String>()
                )
            });
        let workspace_key = self.runtime.runtime_services().workspace_key().to_string();
        SessionMissionOutboxRequest {
            request_id: format!(
                "mission:{workspace_key}:close:{}:{}",
                record.session_id, record.created_at
            ),
            session_id: record.session_id.clone(),
            title,
            workspace_key,
            operation: SessionMissionOutboxOperation::Close,
            created_at_ms: current_time_ms(),
        }
    }

    async fn rollback_created_session(&self, record: &SessionRecord, cause: String) -> String {
        let close = self.close_outbox_request(record);
        match self
            .runtime
            .session_kernel()
            .delete_stored_session_with_mission_outbox(&close)
            .await
        {
            Ok(_) => cause,
            Err(rollback_error) => format!(
                "{cause}; failed to compensate persisted session {}: {rollback_error}",
                record.session_id
            ),
        }
    }

    async fn register_lifecycle(&self, session_id: &str, restored: bool) -> Result<(), String> {
        self.resource_lifecycle.register(session_id).await;
        self.resource_lifecycle.mark_active(session_id).await;
        self.runtime
            .lifecycle_kernel()
            .mark_active(session_id)
            .await
            .map_err(|error| {
                if restored {
                    format!("failed to mark restored session active: {error}")
                } else {
                    format!("failed to mark session active: {error}")
                }
            })?;
        Ok(())
    }

    /// Remove only process-local execution state. Durable transcript and Session
    /// identity remain available for lazy recovery.
    pub(crate) async fn unload_runtime(&self, session_id: &str) -> bool {
        let lock = self.lock_for(session_id).await;
        let _guard = lock.lock().await;
        let removed = self.runtime.remove_active_runtime_if_present(session_id);
        self.resource_lifecycle.unregister(session_id).await;
        removed
    }

    pub(crate) async fn delete_session(&self, session_id: &str) -> Result<bool, String> {
        let lock = self.lock_for(session_id).await;
        let _guard = lock.lock().await;
        let session_kernel = self.runtime.session_kernel();
        let record = session_kernel
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?;
        let removed_active = self.runtime.remove_active_runtime_if_present(session_id);
        self.resource_lifecycle.unregister(session_id).await;
        let Some(record) = record else {
            return Ok(removed_active);
        };
        let request = self.close_outbox_request(&record);
        session_kernel
            .delete_stored_session_with_mission_outbox(&request)
            .await
            .map_err(|error| error.to_string())?;
        Ok(true)
    }

    pub(crate) async fn recover_active_sessions(&self) -> SessionRecoverySummary {
        let mut summary = SessionRecoverySummary::default();
        let mut offset = 0usize;
        let mut records = Vec::new();
        const PAGE_SIZE: usize = 100;
        loop {
            let options = memory::store::session::SessionListOptions {
                query: None,
                model: None,
                status: Some("active"),
                // Recovery activates each page and may update last_activity.
                // Page on immutable creation order so offset pagination cannot
                // skip records that move while earlier pages are restored.
                sort: "created_at",
                order: "asc",
                limit: PAGE_SIZE,
                offset,
            };
            let page = match self
                .runtime
                .session_kernel()
                .list_stored_sessions_page(&options)
                .await
            {
                Ok(Some(page)) => page,
                Ok(None) => break,
                Err(error) => {
                    summary.failed += 1;
                    summary.failures.push(error.to_string());
                    break;
                }
            };
            if page.records.is_empty() {
                break;
            }
            summary.discovered += page.records.len();
            let count = page.records.len();
            records.extend(page.records);
            offset += count;
            if count < PAGE_SIZE {
                break;
            }
        }
        // Finish the stable paged discovery before activation. Runtime
        // construction may update a durable Session's status or timestamps;
        // interleaving those mutations with offset pagination can skip rows.
        let results = stream::iter(records)
            .map(|record| async move {
                let was_active = self.runtime.has_active_session(&record.session_id);
                let request = EnsureSessionRequest::new(
                    &record.session_id,
                    record.model.clone(),
                    SessionSource::Internal,
                );
                let result = self.ensure_session(request).await;
                (record.session_id, was_active, result)
            })
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await;
        for (session_id, was_active, result) in results {
            match result {
                Ok(_) if was_active => summary.already_active += 1,
                Ok(_) => summary.recovered += 1,
                Err(error) => {
                    summary.failed += 1;
                    summary.failures.push(format!("{session_id}: {error}"));
                }
            }
        }
        summary
    }

    /// Apply TTL/idle/capacity policy to hot runtimes without deleting durable
    /// Session identity or transcript data.
    pub(crate) async fn run_resource_cleanup(&self) -> usize {
        let evicted = self.resource_lifecycle.run_cleanup().await;
        let mut terminal = self
            .resource_lifecycle
            .status_snapshot()
            .await
            .into_iter()
            .filter_map(|(session_id, status)| {
                matches!(
                    status,
                    runtime::session_lifecycle::SessionStatus::Expired
                        | runtime::session_lifecycle::SessionStatus::Idle
                        | runtime::session_lifecycle::SessionStatus::Evicted
                )
                .then_some(session_id)
            })
            .collect::<Vec<_>>();
        terminal.extend(evicted);
        terminal.sort();
        terminal.dedup();
        let mut unloaded = 0;
        for session_id in terminal {
            if self.unload_runtime(&session_id).await {
                unloaded += 1;
            }
        }
        unloaded
    }

    #[must_use]
    pub(crate) fn runtime(&self) -> &Arc<RuntimeService> {
        &self.runtime
    }
}

#[async_trait::async_trait]
impl crate::runtime_service::SessionActivationPort for UnifiedSessionManager {
    async fn activate(&self, session_id: &str) -> Result<(), String> {
        self.ensure_session(EnsureSessionRequest::new(
            session_id,
            None,
            SessionSource::Internal,
        ))
        .await
        .map(|_| ())
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_bus::SessionEventBus;
    use crate::gateway::ActiveSessions;
    use crate::session_kernel::SessionKernel;
    use crate::session_lifecycle_kernel::SessionLifecycleKernel;

    fn test_manager(
        max_active_sessions: usize,
    ) -> (
        Arc<UnifiedSessionManager>,
        Arc<memory::UnifiedSessionStore>,
        Arc<ActiveSessions>,
        Arc<SessionLifecycleManager>,
    ) {
        test_manager_with_limits(max_active_sessions, max_active_sessions)
    }

    fn test_manager_with_limits(
        runtime_max_active_sessions: usize,
        manager_max_active_sessions: usize,
    ) -> (
        Arc<UnifiedSessionManager>,
        Arc<memory::UnifiedSessionStore>,
        Arc<ActiveSessions>,
        Arc<SessionLifecycleManager>,
    ) {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let active = Arc::new(ActiveSessions::with_max_sessions(
            runtime_max_active_sessions,
        ));
        let event_bus = SessionEventBus::new();
        let session_kernel = Arc::new(SessionKernel::new(
            Arc::clone(&active),
            Some(Arc::clone(&store)),
            event_bus,
        ));
        let lifecycle_kernel = Arc::new(SessionLifecycleKernel::with_store(Arc::clone(&store)));
        let runtime_services = runtime::RuntimeServices::in_memory().unwrap();
        runtime_services
            .install_session_store(Arc::clone(&store))
            .unwrap();
        let runtime = Arc::new(
            RuntimeService::new(
                Arc::clone(&active),
                Arc::new(session::SessionLeaseRegistry::default()),
                session_kernel,
                lifecycle_kernel,
                std::time::Instant::now(),
                Arc::new(runtime::ProviderRegistry::empty()),
                Arc::new(runtime::UpgradeCoordinator::new()),
                runtime_services,
            )
            .unwrap(),
        );
        let resource_lifecycle = Arc::new(SessionLifecycleManager::new(
            runtime::session_lifecycle::SessionLifecycleConfig::default(),
        ));
        (
            Arc::new(UnifiedSessionManager::new(
                runtime,
                Arc::clone(&resource_lifecycle),
                manager_max_active_sessions,
            )),
            store,
            active,
            resource_lifecycle,
        )
    }

    #[tokio::test]
    async fn ensure_session_creates_hot_durable_and_lifecycle_state() {
        let (manager, store, active, lifecycle) = test_manager(8);
        let outcome = manager
            .ensure_session(EnsureSessionRequest::new(
                "session-new",
                Some("test-model".to_string()),
                SessionSource::WebUi,
            ))
            .await
            .unwrap();

        assert!(outcome.created);
        assert!(!outcome.restored);
        assert!(active.get("session-new").is_some());
        assert!(store.get_session("session-new").await.unwrap().is_some());
        assert_eq!(
            lifecycle.check_session("session-new").await,
            Some(runtime::session_lifecycle::SessionStatus::Active)
        );
        assert_eq!(
            manager
                .runtime()
                .lifecycle_kernel()
                .snapshot("session-new")
                .await
                .unwrap()
                .state,
            session::SessionLifecycleState::Active
        );
    }

    #[tokio::test]
    async fn concurrent_ensure_creates_exactly_one_session() {
        let (manager, store, active, _) = test_manager(32);
        let mut tasks = Vec::new();
        for _ in 0..20 {
            let manager = Arc::clone(&manager);
            tasks.push(tokio::spawn(async move {
                manager
                    .ensure_session(EnsureSessionRequest::new(
                        "session-concurrent",
                        Some("test-model".to_string()),
                        SessionSource::Socket,
                    ))
                    .await
                    .unwrap()
                    .created
            }));
        }
        let mut created = 0;
        for task in tasks {
            created += usize::from(task.await.unwrap());
        }
        assert_eq!(created, 1);
        assert_eq!(active.list(), vec!["session-concurrent".to_string()]);
        assert!(store
            .get_session("session-concurrent")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn activation_failure_compensates_session_and_mission_lifecycle() {
        let (manager, store, active, _) = test_manager_with_limits(1, 2);
        manager
            .ensure_session(EnsureSessionRequest::new(
                "session-capacity-owner",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap();

        let error = manager
            .ensure_session(EnsureSessionRequest::new(
                "session-activation-failure",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap_err();

        assert!(error.contains("sessions limit"), "{error}");
        assert!(active.get("session-activation-failure").is_none());
        assert!(store
            .get_session("session-activation-failure")
            .await
            .unwrap()
            .is_none());
        let records = store
            .claim_session_mission_outbox(
                manager.runtime().runtime_services().workspace_key(),
                "test-worker",
                current_time_ms(),
                1_000,
                16,
            )
            .await
            .unwrap();
        assert!(records.iter().any(|record| {
            record.session_id == "session-activation-failure"
                && record.operation == SessionMissionOutboxOperation::Close
        }));
    }

    #[tokio::test]
    async fn hot_ensure_path_averages_below_five_milliseconds() {
        let (manager, _, _, _) = test_manager(8);
        manager
            .ensure_session(EnsureSessionRequest::new(
                "session-hot",
                None,
                SessionSource::Tui,
            ))
            .await
            .unwrap();
        let started = std::time::Instant::now();
        for _ in 0..100 {
            let outcome = manager
                .ensure_session(EnsureSessionRequest::new(
                    "session-hot",
                    None,
                    SessionSource::Tui,
                ))
                .await
                .unwrap();
            assert!(!outcome.created);
            assert!(!outcome.restored);
        }
        assert!(
            started.elapsed() < std::time::Duration::from_millis(500),
            "100 hot ensures took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn only_external_surfaces_receive_surface_delivery_guidance() {
        assert!(SessionSource::Surface("feishu".to_string())
            .system_prompt()
            .first()
            .is_some_and(|prompt| prompt.contains("feishu")));
        assert!(SessionSource::WebUi.system_prompt().is_empty());
        assert!(SessionSource::Tui.system_prompt().is_empty());
        assert!(SessionSource::Socket.system_prompt().is_empty());
    }

    #[tokio::test]
    async fn unloaded_session_is_recovered_without_losing_durable_state() {
        let (manager, store, active, _) = test_manager(8);
        manager
            .ensure_session(EnsureSessionRequest::new(
                "session-recover",
                Some("test-model".to_string()),
                SessionSource::Cli,
            ))
            .await
            .unwrap();
        assert!(manager.unload_runtime("session-recover").await);
        assert!(active.get("session-recover").is_none());
        assert!(store
            .get_session("session-recover")
            .await
            .unwrap()
            .is_some());

        let outcome = manager
            .ensure_session(EnsureSessionRequest::new(
                "session-recover",
                None,
                SessionSource::Internal,
            ))
            .await
            .unwrap();
        assert!(!outcome.created);
        assert!(outcome.restored);
        assert!(active.get("session-recover").is_some());
    }

    #[tokio::test]
    async fn delete_session_cleans_hot_durable_and_lifecycle_state() {
        let (manager, store, active, lifecycle) = test_manager(8);
        manager
            .ensure_session(EnsureSessionRequest::new(
                "session-delete",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap();
        assert!(manager.delete_session("session-delete").await.unwrap());
        assert!(active.get("session-delete").is_none());
        assert!(store.get_session("session-delete").await.unwrap().is_none());
        assert!(lifecycle.check_session("session-delete").await.is_none());
    }

    #[tokio::test]
    async fn capacity_limit_rejects_new_sessions_but_never_blocks_durable_recovery() {
        let (manager, store, active, lifecycle) = test_manager(1);
        manager
            .ensure_session(EnsureSessionRequest::new(
                "session-one",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap();
        let record = SessionRecord {
            session_id: "session-durable".to_string(),
            platform: "webui".to_string(),
            chat_id: "session-durable".to_string(),
            user_id: None,
            model: Some("test-model".to_string()),
            created_at: Utc::now().to_rfc3339(),
            last_activity: Utc::now().to_rfc3339(),
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        store.upsert_session(&record).await.unwrap();

        let recovered = manager
            .ensure_session(EnsureSessionRequest::new(
                "session-durable",
                None,
                SessionSource::Internal,
            ))
            .await
            .unwrap();
        assert!(recovered.restored);
        assert!(active.get("session-durable").is_some());
        assert_eq!(
            lifecycle.check_session("session-durable").await,
            Some(runtime::session_lifecycle::SessionStatus::Active)
        );
        assert!(manager
            .ensure_session(EnsureSessionRequest::new(
                "session-over-limit",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap_err()
            .contains("active session limit"));
    }

    #[tokio::test]
    async fn cleanup_does_not_remove_a_recent_session_or_its_durable_identity() {
        let (manager, store, active, _) = test_manager(8);
        manager
            .ensure_session(EnsureSessionRequest::new(
                "session-recent",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap();

        assert_eq!(manager.run_resource_cleanup().await, 0);
        assert!(active.get("session-recent").is_some());
        assert!(store.get_session("session-recent").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn idle_cleanup_unloads_only_hot_state_and_preserves_durable_identity() {
        let (base, store, active, _) = test_manager(8);
        let lifecycle = Arc::new(SessionLifecycleManager::new(
            runtime::session_lifecycle::SessionLifecycleConfig {
                idle_timeout: Some(std::time::Duration::from_millis(10)),
                max_ttl: None,
                max_active_sessions: 8,
                eviction_policy: runtime::session_lifecycle::EvictionPolicy::Lru,
                cleanup_interval: std::time::Duration::from_millis(5),
            },
        ));
        let manager = UnifiedSessionManager::new(base.runtime().clone(), lifecycle, 8);
        manager
            .ensure_session(EnsureSessionRequest::new(
                "session-idle-cleanup",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        assert_eq!(manager.run_resource_cleanup().await, 1);
        assert!(active.get("session-idle-cleanup").is_none());
        assert!(store
            .get_session("session-idle-cleanup")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn startup_recovery_pages_through_more_than_one_hundred_sessions() {
        let (manager, store, active, lifecycle) = test_manager(256);
        for index in 0..125 {
            let session_id = format!("session-page-{index:03}");
            let record = SessionRecord {
                session_id: session_id.clone(),
                platform: "webui".to_string(),
                chat_id: session_id,
                user_id: None,
                model: Some("test-model".to_string()),
                created_at: Utc::now().to_rfc3339(),
                last_activity: Utc::now().to_rfc3339(),
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            };
            store.upsert_session(&record).await.unwrap();
        }

        let summary = manager.recover_active_sessions().await;
        assert_eq!(summary.discovered, 125);
        assert_eq!(summary.recovered, 125);
        assert_eq!(summary.already_active, 0);
        assert_eq!(summary.failed, 0, "{:?}", summary.failures);
        assert_eq!(active.list().len(), 125);
        assert_eq!(lifecycle.status_snapshot().await.len(), 125);
    }

    #[tokio::test]
    async fn cold_runtime_bridge_activates_only_through_unified_manager() {
        let (manager, store, active, _) = test_manager(8);
        let record = SessionRecord {
            session_id: "session-cold-bridge".to_string(),
            platform: "surface:test".to_string(),
            chat_id: "session-cold-bridge".to_string(),
            user_id: None,
            model: Some("test-model".to_string()),
            created_at: Utc::now().to_rfc3339(),
            last_activity: Utc::now().to_rfc3339(),
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        store.upsert_session(&record).await.unwrap();
        let activation_port: Arc<dyn crate::runtime_service::SessionActivationPort> =
            manager.clone();
        manager
            .runtime()
            .install_session_activator(Arc::downgrade(&activation_port))
            .unwrap();
        let ingress = memory::SessionRuntimeOutboxRecord {
            request_id: "cold-request".to_string(),
            turn_id: "cold-turn".to_string(),
            message_id: "cold-message".to_string(),
            session_id: record.session_id.clone(),
            sequence: 0,
            status: memory::OutboxStatus::Claimed,
            runtime_commit_cursor: None,
            attempts: 1,
            next_attempt_at_ms: 0,
            claim_owner: Some("test-worker".to_string()),
            claim_expires_at_ms: Some(u64::MAX),
            failure_class: None,
            last_error: None,
            revision: 1,
            created_at_ms: 1,
            updated_at_ms: 1,
        };

        let _ = manager
            .runtime()
            .execute_ingress_record(&ingress, "activation proof")
            .await;
        assert!(active.get(&record.session_id).is_some());
        assert_eq!(
            manager
                .runtime()
                .lifecycle_kernel()
                .snapshot(&record.session_id)
                .await
                .unwrap()
                .state,
            session::SessionLifecycleState::Active
        );
    }
}
