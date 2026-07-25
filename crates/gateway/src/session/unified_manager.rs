use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use chrono::Utc;
use futures::{stream, StreamExt};
use memory::{
    SessionMissionOutboxOperation, SessionMissionOutboxRequest, SessionRecord,
    SessionRecoveryManifest, SessionRecoverySignal,
};
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

    fn source_guidance(&self) -> Vec<String> {
        let mut guidance = vec![format!(
            "# Active surface\nYou are serving this turn through `{}`. The surface changes presentation only; your identity, governed Runtime tools, execution planning, and evidence rules remain Cowd-owned.",
            self.platform()
        )];
        if let Self::Surface(surface) = self {
            guidance.push(format!(
                "你正在通过 `{surface}` 外部 surface 回复用户。必须优先给出可见、简洁、可执行的阶段性结果。\
                如果任务需要读代码、检查 README、调研或测试，只检查足以支撑结论的关键证据；不要进行无边界穷举。\
                如果当前 turn 的信息或时间不足，直接说明已检查内容、当前判断、剩余风险和建议下一步，而不是持续调用工具直到超时。\
                外部 surface 的用户体验要求：宁可给出有证据的阶段性结论，也不能让用户长时间没有任何回复。"
            ));
        }
        guidance
    }

    fn system_prompt(&self) -> Vec<String> {
        self.source_guidance()
            .into_iter()
            .fold(runtime::SystemPromptBuilder::new(), |builder, guidance| {
                builder.with_source_guidance(guidance)
            })
            .build()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EnsureSessionRequest {
    pub(crate) session_id: String,
    pub(crate) model: Option<String>,
    pub(crate) source: SessionSource,
    pub(crate) title: Option<String>,
    pub(crate) user_id: Option<String>,
    pub(crate) owner_principal_id: Option<String>,
    /// Existing records created before principal ownership was introduced are
    /// deliberately fail-closed.  Only a route that has verified an
    /// interactive management capability may request their audited migration.
    pub(crate) allow_legacy_owner_migration: bool,
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
            owner_principal_id: None,
            allow_legacy_owner_migration: false,
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
    pub(crate) metadata_loaded: usize,
    pub(crate) required: usize,
    pub(crate) attached: usize,
    pub(crate) recent: usize,
    pub(crate) recovered: usize,
    pub(crate) already_active: usize,
    pub(crate) failed: usize,
    pub(crate) hot_bytes: u64,
    pub(crate) failures: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SessionHydrationStatus {
    MetadataLoaded,
    Hydrating,
    Ready,
    Degraded,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct SessionWorkingSetEntry {
    pub(crate) session_id: String,
    pub(crate) status: SessionHydrationStatus,
    pub(crate) transcript_bytes: u64,
    pub(crate) transcript_messages: u64,
    pub(crate) last_activity_ms: u64,
    pub(crate) pin_reasons: BTreeSet<String>,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct SessionWorkingSetProjection {
    pub(crate) hot_bytes: u64,
    pub(crate) byte_budget: u64,
    pub(crate) metadata_loaded: usize,
    pub(crate) hydrating: usize,
    pub(crate) ready: usize,
    pub(crate) degraded: usize,
    pub(crate) entries: Vec<SessionWorkingSetEntry>,
}

#[derive(Default)]
struct WorkingSetState {
    hot_bytes: u64,
    entries: HashMap<String, SessionWorkingSetEntry>,
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
    working_set: Mutex<WorkingSetState>,
    max_active_sessions: usize,
    recovery: runtime::SessionRecoveryConfig,
}

impl UnifiedSessionManager {
    #[must_use]
    pub(crate) fn new(
        runtime: Arc<RuntimeService>,
        resource_lifecycle: Arc<SessionLifecycleManager>,
        max_active_sessions: usize,
        recovery: runtime::SessionRecoveryConfig,
    ) -> Self {
        Self {
            runtime,
            resource_lifecycle,
            session_locks: Mutex::new(HashMap::new()),
            working_set: Mutex::new(WorkingSetState::default()),
            max_active_sessions: max_active_sessions.max(1),
            recovery,
        }
    }

    async fn lock_for(&self, session_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.session_locks.lock().await;
        locks
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn register_manifest_metadata(
        &self,
        manifest: &SessionRecoveryManifest,
        extra_pin_reasons: impl IntoIterator<Item = &'static str>,
    ) {
        let mut pin_reasons = manifest_pin_reasons(manifest);
        pin_reasons.extend(extra_pin_reasons.into_iter().map(str::to_string));
        let mut state = self.working_set.lock().await;
        let existing = state.entries.get(&manifest.session_id).cloned();
        let existing_status = existing
            .as_ref()
            .map(|entry| entry.status)
            .unwrap_or(SessionHydrationStatus::MetadataLoaded);
        if existing_status == SessionHydrationStatus::Ready {
            state.hot_bytes = state
                .hot_bytes
                .saturating_sub(existing.as_ref().map_or(0, |entry| entry.transcript_bytes))
                .saturating_add(manifest.transcript_bytes);
        }
        state.entries.insert(
            manifest.session_id.clone(),
            SessionWorkingSetEntry {
                session_id: manifest.session_id.clone(),
                status: existing_status,
                transcript_bytes: manifest.transcript_bytes,
                transcript_messages: manifest.transcript_messages,
                last_activity_ms: manifest.last_activity_ms,
                pin_reasons,
                last_error: None,
            },
        );
    }

    async fn begin_hydration(&self, session_id: &str, manifest: &SessionRecoveryManifest) {
        self.register_manifest_metadata(manifest, []).await;
        let victims = {
            let mut state = self.working_set.lock().await;
            let requested = manifest.transcript_bytes;
            let budget = self.recovery.hot_bytes as u64;
            let mut candidates = state
                .entries
                .values()
                .filter(|entry| {
                    entry.session_id != session_id
                        && entry.status == SessionHydrationStatus::Ready
                        && entry.pin_reasons.is_empty()
                })
                .map(|entry| {
                    (
                        entry.last_activity_ms,
                        entry.session_id.clone(),
                        entry.transcript_bytes,
                    )
                })
                .collect::<Vec<_>>();
            candidates.sort_by_key(|candidate| (candidate.0, candidate.1.clone()));
            let mut victims = Vec::new();
            let mut projected = state.hot_bytes.saturating_add(requested);
            for (_, victim, bytes) in candidates {
                if projected <= budget {
                    break;
                }
                projected = projected.saturating_sub(bytes);
                victims.push(victim);
            }
            for victim in &victims {
                let bytes = state
                    .entries
                    .get(victim)
                    .map_or(0, |entry| entry.transcript_bytes);
                state.hot_bytes = state.hot_bytes.saturating_sub(bytes);
                if let Some(entry) = state.entries.get_mut(victim) {
                    entry.status = SessionHydrationStatus::MetadataLoaded;
                    entry.last_error = None;
                }
            }
            if let Some(entry) = state.entries.get_mut(session_id) {
                entry.status = SessionHydrationStatus::Hydrating;
                entry.last_error = None;
            }
            victims
        };
        for victim in victims {
            self.runtime.remove_active_runtime_if_present(&victim);
            self.resource_lifecycle.unregister(&victim).await;
            tracing::info!(
                session_id = victim,
                "evicted unpinned hot Runtime carrier under byte budget"
            );
        }
    }

    async fn finish_hydration(&self, session_id: &str, result: Result<(), &str>) {
        let mut state = self.working_set.lock().await;
        let previous = state.entries.get(session_id).map(|entry| entry.status);
        let bytes = state
            .entries
            .get(session_id)
            .map_or(0, |entry| entry.transcript_bytes);
        match result {
            Ok(()) => {
                if previous != Some(SessionHydrationStatus::Ready) {
                    state.hot_bytes = state.hot_bytes.saturating_add(bytes);
                }
                if let Some(entry) = state.entries.get_mut(session_id) {
                    entry.status = SessionHydrationStatus::Ready;
                    entry.last_error = None;
                }
            }
            Err(error) => {
                if previous == Some(SessionHydrationStatus::Ready) {
                    state.hot_bytes = state.hot_bytes.saturating_sub(bytes);
                }
                if let Some(entry) = state.entries.get_mut(session_id) {
                    entry.status = SessionHydrationStatus::Degraded;
                    entry.last_error = Some(error.to_string());
                }
            }
        }
    }

    async fn mark_metadata_only(&self, session_id: &str) {
        let mut state = self.working_set.lock().await;
        let bytes = state
            .entries
            .get(session_id)
            .filter(|entry| entry.status == SessionHydrationStatus::Ready)
            .map_or(0, |entry| entry.transcript_bytes);
        state.hot_bytes = state.hot_bytes.saturating_sub(bytes);
        if let Some(entry) = state.entries.get_mut(session_id) {
            entry.status = SessionHydrationStatus::MetadataLoaded;
            entry.last_error = None;
        }
    }

    pub(crate) async fn working_set_projection(&self) -> SessionWorkingSetProjection {
        let state = self.working_set.lock().await;
        let mut projection = SessionWorkingSetProjection {
            hot_bytes: state.hot_bytes,
            byte_budget: self.recovery.hot_bytes as u64,
            ..SessionWorkingSetProjection::default()
        };
        projection.entries = state.entries.values().cloned().collect();
        projection.entries.sort_by_key(|entry| {
            (
                std::cmp::Reverse(entry.last_activity_ms),
                entry.session_id.clone(),
            )
        });
        for entry in &projection.entries {
            match entry.status {
                SessionHydrationStatus::MetadataLoaded => projection.metadata_loaded += 1,
                SessionHydrationStatus::Hydrating => projection.hydrating += 1,
                SessionHydrationStatus::Ready => projection.ready += 1,
                SessionHydrationStatus::Degraded => projection.degraded += 1,
            }
        }
        projection
    }

    async fn refresh_working_set_signals(&self) {
        let pending = self
            .runtime
            .runtime_services()
            .approval_queue()
            .pending()
            .into_iter()
            .filter_map(|approval| approval.source.session_id)
            .collect::<BTreeSet<_>>();
        let continuations = self
            .runtime
            .running_session_execution_indices()
            .into_iter()
            .map(|index| index.session_id)
            .collect::<BTreeSet<_>>();
        let writer_leases = self
            .runtime
            .active_lease_session_ids()
            .await
            .into_iter()
            .collect::<BTreeSet<_>>();
        let session_ids = self
            .working_set
            .lock()
            .await
            .entries
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for session_id in session_ids {
            let Ok(Some(mut manifest)) = self
                .runtime
                .session_kernel()
                .stored_recovery_manifest(&session_id)
                .await
            else {
                continue;
            };
            let pending_approval = pending.contains(&session_id);
            if manifest.pending_approval != pending_approval {
                if let Ok(Some(updated)) = self
                    .runtime
                    .session_kernel()
                    .set_recovery_signal(
                        &session_id,
                        SessionRecoverySignal::PendingApproval,
                        pending_approval,
                        current_time_ms(),
                    )
                    .await
                {
                    manifest = updated;
                }
            }
            if continuations.contains(&session_id) && !manifest.mission_agent_team_continuation {
                if let Ok(Some(updated)) = self
                    .runtime
                    .session_kernel()
                    .set_recovery_signal(
                        &session_id,
                        SessionRecoverySignal::MissionAgentTeamContinuation,
                        true,
                        current_time_ms(),
                    )
                    .await
                {
                    manifest = updated;
                }
            }
            let extra_reasons = writer_leases
                .contains(&session_id)
                .then_some("writer_lease")
                .into_iter();
            self.register_manifest_metadata(&manifest, extra_reasons)
                .await;
        }
    }

    async fn is_pinned(&self, session_id: &str) -> bool {
        self.working_set
            .lock()
            .await
            .entries
            .get(session_id)
            .is_some_and(|entry| !entry.pin_reasons.is_empty())
    }

    async fn evict_one_unpinned_for_capacity(&self, exclude: &str) -> Option<String> {
        let victim = {
            let mut state = self.working_set.lock().await;
            let victim = state
                .entries
                .values()
                .filter(|entry| {
                    entry.session_id != exclude
                        && entry.status == SessionHydrationStatus::Ready
                        && entry.pin_reasons.is_empty()
                })
                .min_by_key(|entry| (entry.last_activity_ms, entry.session_id.clone()))
                .map(|entry| entry.session_id.clone())?;
            let bytes = state
                .entries
                .get(&victim)
                .map_or(0, |entry| entry.transcript_bytes);
            state.hot_bytes = state.hot_bytes.saturating_sub(bytes);
            if let Some(entry) = state.entries.get_mut(&victim) {
                entry.status = SessionHydrationStatus::MetadataLoaded;
            }
            victim
        };
        self.runtime.remove_active_runtime_if_present(&victim);
        self.resource_lifecycle.unregister(&victim).await;
        Some(victim)
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
        let mut existing = session_kernel
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?;
        if let Some(requested_owner) = request.owner_principal_id.as_deref() {
            if let Some(record) = existing.as_mut() {
                let existing_owner = record
                    .metadata_json
                    .as_deref()
                    .and_then(|metadata| serde_json::from_str::<serde_json::Value>(metadata).ok())
                    .and_then(|metadata| {
                        metadata
                            .get("owner_principal_id")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_string)
                    });
                if existing_owner
                    .as_deref()
                    .is_some_and(|owner| owner != requested_owner)
                {
                    return Err("session is owned by another authenticated principal".to_string());
                }
                if existing_owner.is_none() {
                    if !request.allow_legacy_owner_migration {
                        return Err(
                            "legacy session has no owner and requires privileged owner migration"
                                .to_string(),
                        );
                    }
                    let mut metadata = record
                        .metadata_json
                        .as_deref()
                        .and_then(|metadata| {
                            serde_json::from_str::<serde_json::Value>(metadata).ok()
                        })
                        .and_then(|metadata| metadata.as_object().cloned())
                        .unwrap_or_default();
                    metadata.insert(
                        "owner_principal_id".to_string(),
                        serde_json::Value::String(requested_owner.to_string()),
                    );
                    metadata.insert(
                        "owner_migration".to_string(),
                        serde_json::json!({
                            "kind": "privileged_legacy_claim_v1",
                            "claimed_by": requested_owner,
                            "claimed_at": Utc::now().to_rfc3339(),
                        }),
                    );
                    record.metadata_json = Some(serde_json::Value::Object(metadata).to_string());
                    session_kernel
                        .update_stored_session(record)
                        .await
                        .map_err(|error| error.to_string())?;
                }
            }
        }

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
            let manifest = self
                .runtime
                .session_kernel()
                .stored_recovery_manifest(session_id)
                .await
                .map_err(|error| error.to_string())?
                .unwrap_or_else(|| manifest_from_record(&record));
            self.register_manifest_metadata(&manifest, []).await;
            self.finish_hydration(session_id, Ok(())).await;
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
            self.refresh_working_set_signals().await;
            self.evict_one_unpinned_for_capacity(session_id)
                .await
                .ok_or_else(|| {
                    format!(
                        "active session limit {} reached and all hot Runtime carriers are pinned",
                        self.max_active_sessions
                    )
                })?;
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
            self.refresh_working_set_signals().await;
            let victim = self
                .evict_one_unpinned_for_capacity(session_id)
                .await
                .ok_or_else(|| {
                    "active Session registry is full and all hot Runtime carriers are pinned"
                        .to_string()
                })?;
            tracing::info!(session_id = victim, "evicted unpinned hot Runtime carrier");
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

        let manifest = self
            .runtime
            .session_kernel()
            .stored_recovery_manifest(session_id)
            .await
            .map_err(|error| error.to_string())?
            .unwrap_or_else(|| manifest_from_record(&record));
        self.begin_hydration(session_id, &manifest).await;
        if let Err(error) = self
            .runtime
            .activate_persisted_session(
                session_id,
                Some(&model),
                request.source.system_prompt(),
                self.recovery,
            )
            .await
        {
            self.finish_hydration(session_id, Err(&error)).await;
            if created {
                return Err(self.rollback_created_session(&record, error).await);
            }
            return Err(error);
        }
        self.finish_hydration(session_id, Ok(())).await;
        if let Err(error) = self.register_lifecycle(session_id, !created).await {
            self.runtime.remove_active_runtime_if_present(session_id);
            self.mark_metadata_only(session_id).await;
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
        if let Some(owner) = request.owner_principal_id.as_ref() {
            metadata.insert(
                "owner_principal_id".to_string(),
                serde_json::Value::String(owner.clone()),
            );
        }
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
        self.mark_metadata_only(session_id).await;
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
        let mut state = self.working_set.lock().await;
        if let Some(entry) = state.entries.remove(session_id) {
            if entry.status == SessionHydrationStatus::Ready {
                state.hot_bytes = state.hot_bytes.saturating_sub(entry.transcript_bytes);
            }
        }
        Ok(true)
    }

    pub(crate) async fn recover_active_sessions(&self) -> SessionRecoverySummary {
        let mut summary = SessionRecoverySummary::default();
        let mut offset = 0usize;
        let mut manifests = Vec::new();
        let page_size = self.recovery.manifest_page_size;
        loop {
            let page = match self
                .runtime
                .session_kernel()
                .active_recovery_manifests(offset, page_size)
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
            if page.is_empty() {
                break;
            }
            summary.discovered += page.len();
            let count = page.len();
            manifests.extend(page);
            offset += count;
            if count < page_size {
                break;
            }
        }

        let pending_approval_sessions = self
            .runtime
            .runtime_services()
            .approval_queue()
            .pending()
            .into_iter()
            .filter_map(|approval| approval.source.session_id)
            .collect::<BTreeSet<_>>();
        let continuation_sessions = self
            .runtime
            .running_session_execution_indices()
            .into_iter()
            .map(|index| index.session_id)
            .collect::<BTreeSet<_>>();

        for manifest in &mut manifests {
            let pending_approval = pending_approval_sessions.contains(&manifest.session_id);
            if manifest.pending_approval != pending_approval {
                match self
                    .runtime
                    .session_kernel()
                    .set_recovery_signal(
                        &manifest.session_id,
                        SessionRecoverySignal::PendingApproval,
                        pending_approval,
                        current_time_ms(),
                    )
                    .await
                {
                    Ok(Some(updated)) => *manifest = updated,
                    Ok(None) => {}
                    Err(error) => {
                        summary.failed += 1;
                        summary.failures.push(format!(
                            "{}: failed to reconcile approval manifest: {error}",
                            manifest.session_id
                        ));
                    }
                }
            }
            if continuation_sessions.contains(&manifest.session_id)
                && !manifest.mission_agent_team_continuation
            {
                match self
                    .runtime
                    .session_kernel()
                    .set_recovery_signal(
                        &manifest.session_id,
                        SessionRecoverySignal::MissionAgentTeamContinuation,
                        true,
                        current_time_ms(),
                    )
                    .await
                {
                    Ok(Some(updated)) => *manifest = updated,
                    Ok(None) => {}
                    Err(error) => {
                        summary.failed += 1;
                        summary.failures.push(format!(
                            "{}: failed to reconcile continuation manifest: {error}",
                            manifest.session_id
                        ));
                    }
                }
            }
            self.register_manifest_metadata(manifest, []).await;
            summary.metadata_loaded += 1;
        }

        let now_ms = current_time_ms();
        let recent_cutoff = now_ms.saturating_sub(self.recovery.recent_window_ms);
        let mut required = Vec::new();
        let mut attached = Vec::new();
        let mut recent = Vec::new();
        for manifest in manifests {
            if manifest.in_flight_turn
                || manifest.pending_approval
                || manifest.mission_agent_team_continuation
            {
                summary.required += 1;
                required.push(manifest);
            } else if manifest.active_writer_or_attachment {
                attached.push(manifest);
            } else if manifest.last_activity_ms >= recent_cutoff {
                recent.push(manifest);
            }
        }
        attached.sort_by_key(|manifest| {
            (
                std::cmp::Reverse(manifest.last_activity_ms),
                manifest.session_id.clone(),
            )
        });
        recent.sort_by_key(|manifest| {
            (
                std::cmp::Reverse(manifest.last_activity_ms),
                manifest.session_id.clone(),
            )
        });
        let mut attached_bytes = 0u64;
        let mut selected_attached = Vec::new();
        for manifest in attached {
            let projected = attached_bytes.saturating_add(manifest.transcript_bytes);
            if projected > self.recovery.attached_bytes as u64 {
                continue;
            }
            attached_bytes = projected;
            summary.attached += 1;
            selected_attached.push(manifest);
        }
        let mut recent_bytes = 0u64;
        let mut selected_recent = Vec::new();
        for manifest in recent {
            let projected = recent_bytes.saturating_add(manifest.transcript_bytes);
            if projected > self.recovery.recent_bytes as u64 {
                continue;
            }
            recent_bytes = projected;
            summary.recent += 1;
            selected_recent.push(manifest);
        }
        let selected = required
            .into_iter()
            .chain(selected_attached)
            .chain(selected_recent);
        let results = stream::iter(selected)
            .map(|manifest| async move {
                let was_active = self.runtime.has_active_session(&manifest.session_id);
                let request =
                    EnsureSessionRequest::new(&manifest.session_id, None, SessionSource::Internal);
                let result = self.ensure_session(request).await;
                (manifest.session_id, was_active, result)
            })
            .buffer_unordered(self.recovery.hydrate_concurrency)
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
        summary.hot_bytes = self.working_set.lock().await.hot_bytes;
        summary
    }

    /// Apply TTL/idle/capacity policy to hot runtimes without deleting durable
    /// Session identity or transcript data.
    pub(crate) async fn run_resource_cleanup(&self) -> usize {
        self.refresh_working_set_signals().await;
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
            if self.is_pinned(&session_id).await {
                self.resource_lifecycle.register(&session_id).await;
                self.resource_lifecycle.mark_active(&session_id).await;
                continue;
            }
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

fn manifest_pin_reasons(manifest: &SessionRecoveryManifest) -> BTreeSet<String> {
    let mut reasons = BTreeSet::new();
    if manifest.in_flight_turn {
        reasons.insert("in_flight_turn".to_string());
    }
    if manifest.pending_approval {
        reasons.insert("pending_approval".to_string());
    }
    if manifest.active_writer_or_attachment {
        reasons.insert("writer_or_attachment".to_string());
    }
    if manifest.mission_agent_team_continuation {
        reasons.insert("mission_agent_team_continuation".to_string());
    }
    reasons
}

fn manifest_from_record(record: &SessionRecord) -> SessionRecoveryManifest {
    SessionRecoveryManifest {
        session_id: record.session_id.clone(),
        durable_cursor: record.message_count.max(0) as u64,
        history_revision: 0,
        transcript_messages: record.message_count.max(0) as u64,
        transcript_bytes: 0,
        in_flight_turn: false,
        pending_approval: false,
        active_writer_or_attachment: false,
        mission_agent_team_continuation: false,
        last_activity_ms: 0,
        manifest_revision: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_bus::SessionEventBus;
    use crate::gateway::ActiveSessions;
    use crate::session_kernel::SessionKernel;
    use crate::session_lifecycle_kernel::SessionLifecycleKernel;
    use model_protocol::provider_config::{ProviderConfig, ProvidersConfig};

    fn test_provider_registry() -> Arc<runtime::ProviderRegistry> {
        Arc::new(
            runtime::ProviderRegistry::new(ProvidersConfig {
                providers: HashMap::from([(
                    "test".to_string(),
                    ProviderConfig {
                        name: "test".to_string(),
                        base_url: "http://127.0.0.1:9/v1".to_string(),
                        api_key: "test".to_string(),
                        models: vec![crate::DEFAULT_MODEL.to_string(), "test-model".to_string()],
                        protocol: Some("completions".to_string()),
                    },
                )]),
            })
            .expect("valid inert test provider registry"),
        )
    }

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
                test_provider_registry(),
                Arc::new(runtime::UpgradeCoordinator::new()),
                runtime_services,
            )
            .unwrap(),
        );
        let resource_lifecycle = Arc::new(SessionLifecycleManager::new(
            runtime::session_lifecycle::SessionLifecycleConfig::default(),
        ));
        let recovery = runtime::SessionRecoveryConfig {
            recent_window_ms: 0,
            ..runtime::SessionRecoveryConfig::default()
        };
        (
            Arc::new(UnifiedSessionManager::new(
                runtime,
                Arc::clone(&resource_lifecycle),
                manager_max_active_sessions,
                recovery,
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
    async fn foreign_owner_is_rejected_before_runtime_or_lifecycle_activation() {
        let (manager, store, active, lifecycle) = test_manager(8);
        let mut owner_request = EnsureSessionRequest::new(
            "session-owned",
            Some("test-model".to_string()),
            SessionSource::Tui,
        );
        owner_request.owner_principal_id = Some("principal-a".to_string());
        manager.ensure_session(owner_request).await.unwrap();
        assert!(manager.unload_runtime("session-owned").await);
        assert!(active.get("session-owned").is_none());
        assert!(lifecycle.check_session("session-owned").await.is_none());

        let mut foreign_request =
            EnsureSessionRequest::new("session-owned", None, SessionSource::MissionControl);
        foreign_request.owner_principal_id = Some("principal-b".to_string());
        let error = manager.ensure_session(foreign_request).await.unwrap_err();

        assert!(error.contains("owned by another"), "{error}");
        assert!(active.get("session-owned").is_none());
        assert!(lifecycle.check_session("session-owned").await.is_none());
        let record = store
            .get_session("session-owned")
            .await
            .unwrap()
            .expect("owned durable record");
        assert_eq!(
            record
                .metadata_json
                .as_deref()
                .and_then(|metadata| serde_json::from_str::<serde_json::Value>(metadata).ok())
                .and_then(|metadata| metadata["owner_principal_id"].as_str().map(str::to_string))
                .as_deref(),
            Some("principal-a")
        );
    }

    #[tokio::test]
    async fn legacy_ownerless_session_requires_privileged_audited_migration() {
        let (manager, store, active, lifecycle) = test_manager(8);
        manager
            .ensure_session(EnsureSessionRequest::new(
                "session-legacy-ownerless",
                Some("test-model".to_string()),
                SessionSource::WebUi,
            ))
            .await
            .expect("legacy record fixture");
        assert!(manager.unload_runtime("session-legacy-ownerless").await);

        let mut ordinary =
            EnsureSessionRequest::new("session-legacy-ownerless", None, SessionSource::Tui);
        ordinary.owner_principal_id = Some("ordinary-principal".to_string());
        let error = manager.ensure_session(ordinary).await.unwrap_err();
        assert!(
            error.contains("requires privileged owner migration"),
            "{error}"
        );
        assert!(active.get("session-legacy-ownerless").is_none());
        assert!(lifecycle
            .check_session("session-legacy-ownerless")
            .await
            .is_none());

        let mut privileged =
            EnsureSessionRequest::new("session-legacy-ownerless", None, SessionSource::Tui);
        privileged.owner_principal_id = Some("manager-principal".to_string());
        privileged.allow_legacy_owner_migration = true;
        manager
            .ensure_session(privileged)
            .await
            .expect("privileged migration succeeds");
        let record = store
            .get_session("session-legacy-ownerless")
            .await
            .unwrap()
            .expect("migrated record");
        let metadata: serde_json::Value =
            serde_json::from_str(record.metadata_json.as_deref().expect("metadata"))
                .expect("metadata json");
        assert_eq!(metadata["owner_principal_id"], "manager-principal");
        assert_eq!(
            metadata["owner_migration"]["kind"],
            "privileged_legacy_claim_v1"
        );
        assert_eq!(
            metadata["owner_migration"]["claimed_by"],
            "manager-principal"
        );
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
    fn every_surface_receives_cowd_identity_and_only_external_surfaces_receive_delivery_guidance() {
        for source in [
            SessionSource::WebUi,
            SessionSource::Tui,
            SessionSource::Socket,
            SessionSource::Cli,
            SessionSource::Internal,
            SessionSource::MissionControl,
        ] {
            let prompt = source.system_prompt().join("\n");
            assert!(prompt.contains("You are Cowd"));
            assert!(prompt.contains(runtime::COWD_IDENTITY_CONTRACT_VERSION));
            assert!(!prompt.contains("外部 surface 的用户体验要求"));
        }
        let surface_prompt = SessionSource::Surface("feishu".to_string())
            .system_prompt()
            .join("\n");
        assert!(surface_prompt.contains("You are Cowd"));
        assert!(surface_prompt.contains(runtime::COWD_IDENTITY_CONTRACT_VERSION));
        assert!(surface_prompt.contains("feishu"));
        assert!(surface_prompt.contains("外部 surface 的用户体验要求"));
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
    async fn capacity_limit_evicts_only_hot_state_and_never_blocks_durable_recovery() {
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
        let replacement = manager
            .ensure_session(EnsureSessionRequest::new(
                "session-over-limit",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap();
        assert!(replacement.created);
        assert!(active.get("session-over-limit").is_some());
        assert!(active.get("session-durable").is_none());
        assert!(store
            .get_session("session-durable")
            .await
            .unwrap()
            .is_some());
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
        let manager = UnifiedSessionManager::new(
            base.runtime().clone(),
            lifecycle,
            8,
            runtime::SessionRecoveryConfig::default(),
        );
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
    async fn writer_lease_pins_hot_runtime_without_becoming_restart_durable() {
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
        let manager = UnifiedSessionManager::new(
            base.runtime().clone(),
            lifecycle,
            8,
            runtime::SessionRecoveryConfig::default(),
        );
        manager
            .ensure_session(EnsureSessionRequest::new(
                "session-writer-pin",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap();
        assert_eq!(
            manager
                .runtime()
                .acquire_session_lease_value("session-writer-pin", "web:test", "collaborative",)
                .await["ok"],
            true
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        assert_eq!(manager.run_resource_cleanup().await, 0);
        assert!(active.get("session-writer-pin").is_some());
        let projection = manager.working_set_projection().await;
        let entry = projection
            .entries
            .iter()
            .find(|entry| entry.session_id == "session-writer-pin")
            .unwrap();
        assert!(entry.pin_reasons.contains("writer_lease"));
        assert!(
            !store
                .get_session_recovery_manifest("session-writer-pin")
                .await
                .unwrap()
                .unwrap()
                .active_writer_or_attachment
        );
    }

    #[tokio::test]
    async fn startup_recovery_pages_metadata_without_hydrating_idle_transcripts() {
        let (manager, store, active, lifecycle) = test_manager(256);
        for index in 0..125 {
            let session_id = format!("session-page-{index:03}");
            let record = SessionRecord {
                session_id: session_id.clone(),
                platform: "webui".to_string(),
                chat_id: session_id,
                user_id: None,
                model: Some("test-model".to_string()),
                created_at: "1970-01-01T00:00:00Z".to_string(),
                last_activity: "1970-01-01T00:00:00Z".to_string(),
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
        assert_eq!(summary.metadata_loaded, 125);
        assert_eq!(summary.recovered, 0);
        assert_eq!(summary.already_active, 0);
        assert_eq!(summary.failed, 0, "{:?}", summary.failures);
        assert_eq!(active.list().len(), 0);
        assert_eq!(lifecycle.status_snapshot().await.len(), 0);
        assert_eq!(manager.runtime().hydration_stats().attempts, 0);
        assert_eq!(manager.runtime().hydration_stats().body_reads, 0);
        let projection = manager.working_set_projection().await;
        assert_eq!(projection.metadata_loaded, 125);
        assert_eq!(projection.ready, 0);
    }

    #[tokio::test]
    async fn startup_recovery_hydrates_and_pins_in_flight_session() {
        let (manager, store, active, _) = test_manager(16);
        let record = SessionRecord {
            session_id: "session-required".to_string(),
            platform: "webui".to_string(),
            chat_id: "session-required".to_string(),
            user_id: None,
            model: Some("test-model".to_string()),
            created_at: "1970-01-01T00:00:00Z".to_string(),
            last_activity: "1970-01-01T00:00:00Z".to_string(),
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        store.upsert_session(&record).await.unwrap();
        store
            .append_ingress_with_runtime_outbox(
                "session-required",
                "user",
                Some(r#"[{"type":"text","text":"resume"}]"#),
                1,
                &memory::SessionRuntimeOutboxRequest {
                    request_id: "request-required".to_string(),
                    turn_id: "turn-required".to_string(),
                    message_id: "message-required".to_string(),
                    created_at_ms: 1,
                    runtime_options_json: None,
                },
            )
            .await
            .unwrap();

        let summary = manager.recover_active_sessions().await;
        assert_eq!(summary.required, 1);
        assert_eq!(summary.recovered, 1);
        assert!(active.get("session-required").is_some());
        let stats = manager.runtime().hydration_stats();
        assert_eq!(stats.attempts, 1);
        assert_eq!(stats.body_reads, 1);
        let projection = manager.working_set_projection().await;
        let entry = projection
            .entries
            .iter()
            .find(|entry| entry.session_id == "session-required")
            .unwrap();
        assert_eq!(entry.status, SessionHydrationStatus::Ready);
        assert!(entry.pin_reasons.contains("in_flight_turn"));
    }

    #[tokio::test]
    async fn startup_recovery_reconciles_pending_approval_before_selection() {
        let (manager, store, active, _) = test_manager(16);
        store
            .upsert_session(&SessionRecord {
                session_id: "session-approval".to_string(),
                platform: "webui".to_string(),
                chat_id: "session-approval".to_string(),
                user_id: None,
                model: Some("test-model".to_string()),
                created_at: "1970-01-01T00:00:00Z".to_string(),
                last_activity: "1970-01-01T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        manager
            .runtime()
            .runtime_services()
            .approval_queue()
            .submit(runtime::SubmitGlobalApprovalRequest {
                source: runtime::ApprovalSource {
                    kind: runtime::ApprovalSourceKind::Session,
                    session_id: Some("session-approval".to_string()),
                    agent_id: None,
                    team_id: None,
                    mission_id: None,
                    resource_ref: None,
                    review_ref: None,
                    application: None,
                },
                action: "write".to_string(),
                summary: "approval required".to_string(),
                risk: harness_contract::core::TaskRisk::High,
                evidence_refs: Vec::new(),
                timeout_policy: runtime::ApprovalTimeoutPolicy::Pending,
            })
            .unwrap();

        let summary = manager.recover_active_sessions().await;
        assert_eq!(summary.required, 1);
        assert_eq!(summary.recovered, 1);
        assert!(active.get("session-approval").is_some());
        let manifest = store
            .get_session_recovery_manifest("session-approval")
            .await
            .unwrap()
            .unwrap();
        assert!(manifest.pending_approval);
    }

    #[tokio::test]
    async fn startup_recovery_hydrates_durable_attachment_and_mission_continuation() {
        let (manager, store, active, _) = test_manager(16);
        for session_id in ["session-attached", "session-mission"] {
            store
                .upsert_session(&SessionRecord {
                    session_id: session_id.to_string(),
                    platform: "webui".to_string(),
                    chat_id: session_id.to_string(),
                    user_id: None,
                    model: Some("test-model".to_string()),
                    created_at: "1970-01-01T00:00:00Z".to_string(),
                    last_activity: "1970-01-01T00:00:00Z".to_string(),
                    message_count: 0,
                    reset_policy: "manual".to_string(),
                    metadata_json: None,
                    input_tokens: 0,
                    output_tokens: 0,
                    estimated_cost_usd: 0.0,
                    status: "active".to_string(),
                })
                .await
                .unwrap();
        }
        manager
            .runtime()
            .lifecycle_kernel()
            .attach(
                "session-attached",
                session::SessionActor::new("web:test", "webui"),
            )
            .await
            .unwrap();
        store
            .upsert_session_with_mission_outbox(
                &store.get_session("session-mission").await.unwrap().unwrap(),
                &SessionMissionOutboxRequest {
                    request_id: "mission-start-recovery".to_string(),
                    session_id: "session-mission".to_string(),
                    title: "mission".to_string(),
                    workspace_key: "workspace".to_string(),
                    operation: SessionMissionOutboxOperation::Start,
                    created_at_ms: 1,
                },
            )
            .await
            .unwrap();

        let summary = manager.recover_active_sessions().await;
        assert_eq!(summary.required, 1);
        assert_eq!(summary.attached, 1);
        assert_eq!(summary.recovered, 2);
        assert!(active.get("session-attached").is_some());
        assert!(active.get("session-mission").is_some());
        let projection = manager.working_set_projection().await;
        let attached = projection
            .entries
            .iter()
            .find(|entry| entry.session_id == "session-attached")
            .unwrap();
        assert!(attached.pin_reasons.contains("writer_or_attachment"));
        let mission = projection
            .entries
            .iter()
            .find(|entry| entry.session_id == "session-mission")
            .unwrap();
        assert!(mission
            .pin_reasons
            .contains("mission_agent_team_continuation"));
    }

    #[tokio::test]
    async fn concurrent_cold_attach_hydrates_transcript_once() {
        let (manager, store, active, _) = test_manager(64);
        let record = SessionRecord {
            session_id: "session-single-flight".to_string(),
            platform: "webui".to_string(),
            chat_id: "session-single-flight".to_string(),
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
        store
            .insert_message(&memory::SessionMessage {
                stable_message_id: "single-flight-message".to_string(),
                session_id: "session-single-flight".to_string(),
                sequence: 0,
                role: "user".to_string(),
                content_json: r#"[{"type":"text","text":"history"}]"#.to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: 1,
            })
            .await
            .unwrap();

        let results = futures::future::join_all((0..32).map(|_| {
            let manager = Arc::clone(&manager);
            async move {
                manager
                    .ensure_session(EnsureSessionRequest::new(
                        "session-single-flight",
                        None,
                        SessionSource::WebUi,
                    ))
                    .await
            }
        }))
        .await;
        assert!(results.into_iter().all(|result| result.is_ok()));
        assert!(active.get("session-single-flight").is_some());
        let stats = manager.runtime().hydration_stats();
        assert_eq!(stats.attempts, 1);
        assert_eq!(stats.body_reads, 1);
    }

    #[tokio::test]
    async fn byte_budget_evicts_unpinned_hot_runtime_but_keeps_durable_history() {
        let (base, store, active, _) = test_manager(8);
        let lifecycle = Arc::new(SessionLifecycleManager::default());
        let manager = UnifiedSessionManager::new(
            base.runtime().clone(),
            Arc::clone(&lifecycle),
            8,
            runtime::SessionRecoveryConfig {
                hot_bytes: 1,
                attached_bytes: 0,
                recent_bytes: 0,
                recent_window_ms: 0,
                ..runtime::SessionRecoveryConfig::default()
            },
        );
        for session_id in ["session-byte-a", "session-byte-b"] {
            store
                .upsert_session(&SessionRecord {
                    session_id: session_id.to_string(),
                    platform: "webui".to_string(),
                    chat_id: session_id.to_string(),
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
                })
                .await
                .unwrap();
            store
                .insert_message(&memory::SessionMessage {
                    stable_message_id: format!("{session_id}-message"),
                    session_id: session_id.to_string(),
                    sequence: 0,
                    role: "user".to_string(),
                    content_json: r#"[{"type":"text","text":"payload"}]"#.to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: 1,
                })
                .await
                .unwrap();
        }
        manager
            .ensure_session(EnsureSessionRequest::new(
                "session-byte-a",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap();
        manager
            .ensure_session(EnsureSessionRequest::new(
                "session-byte-b",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap();

        assert!(active.get("session-byte-a").is_none());
        assert!(active.get("session-byte-b").is_some());
        assert!(store.get_session("session-byte-a").await.unwrap().is_some());
        let projection = manager.working_set_projection().await;
        assert_eq!(projection.ready, 1);
        assert_eq!(projection.metadata_loaded, 1);
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
            runtime_options_json: None,
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
