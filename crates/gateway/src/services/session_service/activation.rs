use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Weak};

use chrono::Utc;
use futures::{stream, StreamExt};
use runtime::session_lifecycle::SessionWorkingSetManager;
use session::{SessionRecord, SessionRecoveryManifest, SessionRecoverySignal};
use tokio::sync::Mutex;

use crate::runtime_service::RuntimeService;
use crate::services::session_service::presence::SessionPresenceLedger;
use crate::services::session_service::repository::SessionRepository;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionActivationIntent {
    CreateNew,
    Ensure,
    ExistingOnly,
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
    pub(crate) metadata_only: usize,
    pub(crate) model_rebind_required: usize,
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
    pub(crate) reserved_bytes: u64,
    pub(crate) evicting_bytes: u64,
    pub(crate) byte_budget: u64,
    pub(crate) concurrent_hydrations: usize,
    pub(crate) metadata_loaded: usize,
    pub(crate) hydrating: usize,
    pub(crate) ready: usize,
    pub(crate) degraded: usize,
    pub(crate) entries: Vec<SessionWorkingSetEntry>,
}

#[derive(Default)]
struct WorkingSetState {
    hot_bytes: u64,
    reserved_bytes: u64,
    evicting_bytes: u64,
    concurrent_hydrations: usize,
    entries: HashMap<String, SessionWorkingSetEntry>,
}

/// Exclusive coordinator for durable Session identity and process-local Runtime state.
///
/// RuntimeService remains the low-level Runtime factory/cache. SessionRepository remains
/// the durable store boundary. Callers must use this manager so those resources and
/// both lifecycle projections change as one operation.
pub(crate) struct SessionActivationCoordinator {
    runtime: Arc<RuntimeService>,
    repository: Arc<SessionRepository>,
    presence_ledger: Arc<SessionPresenceLedger>,
    resource_lifecycle: Arc<SessionWorkingSetManager>,
    session_locks: Mutex<HashMap<String, Weak<Mutex<()>>>>,
    context_indexing: Mutex<HashSet<String>>,
    working_set: Mutex<WorkingSetState>,
    max_active_sessions: Option<usize>,
    recovery: runtime::SessionRecoveryConfig,
}

impl SessionActivationCoordinator {
    pub(crate) async fn activate_existing(
        &self,
        request: EnsureSessionRequest,
    ) -> Result<EnsureSessionOutcome, String> {
        self.activate(request, SessionActivationIntent::ExistingOnly)
            .await
    }

    #[must_use]
    pub(crate) fn new(
        runtime: Arc<RuntimeService>,
        repository: Arc<SessionRepository>,
        presence_ledger: Arc<SessionPresenceLedger>,
        resource_lifecycle: Arc<SessionWorkingSetManager>,
        max_active_sessions: Option<usize>,
        recovery: runtime::SessionRecoveryConfig,
    ) -> Self {
        Self {
            runtime,
            repository,
            presence_ledger,
            resource_lifecycle,
            session_locks: Mutex::new(HashMap::new()),
            context_indexing: Mutex::new(HashSet::new()),
            working_set: Mutex::new(WorkingSetState::default()),
            max_active_sessions: max_active_sessions.map(|value| value.max(1)),
            recovery,
        }
    }

    /// Coalesce rebuild triggers per Session while durable outbox state keeps
    /// the operation recoverable across process restarts.
    pub(crate) fn schedule_context_index_reconciliation(
        self: &Arc<Self>,
        session_id: impl Into<String>,
    ) {
        let coordinator = Arc::clone(self);
        let session_id = session_id.into();
        coordinator
            .runtime
            .runtime_services()
            .hot_state()
            .sessions()
            .invalidate_context(&session_id);
        tokio::spawn(async move {
            {
                let mut indexing = coordinator.context_indexing.lock().await;
                if !indexing.insert(session_id.clone()) {
                    return;
                }
            }
            let history = coordinator.repository.history_reader();
            let outcome = match history.as_ref() {
                Some(history) => {
                    let outcome = history
                        .reconcile_context_index(
                            &session_id,
                            coordinator.recovery.context_index_card_span,
                            coordinator.recovery.context_index_parent_span,
                            Utc::now().timestamp_millis().max(0) as u64,
                        )
                        .await
                        .map(|coverage| coverage.complete)
                        .map_err(|error| error.to_string());
                    if matches!(outcome, Ok(true)) {
                        match history
                            .page_in_context(
                                &session_id,
                                coordinator.recovery.context_card_cache_entries,
                            )
                            .await
                        {
                            Ok(Some(page)) => {
                                let projection_generation = page.manifest.projection_generation;
                                coordinator
                                    .runtime
                                    .runtime_services()
                                    .hot_state()
                                    .sessions()
                                    .update(&session_id, |snapshot| {
                                        snapshot.context_manifest = Some(page.manifest);
                                        snapshot.context_cards = page.context_cards;
                                        snapshot.context_refs.retain(|reference| {
                                            !reference.starts_with("session-context:")
                                        });
                                        snapshot.context_refs.push(format!(
                                            "session-context:{session_id}:{projection_generation}"
                                        ));
                                    });
                            }
                            Ok(None) => {}
                            Err(error) => tracing::warn!(
                                session_id,
                                %error,
                                "Session context hot snapshot refresh failed after reconciliation"
                            ),
                        }
                    }
                    outcome
                }
                None => Err("canonical Session history reader is unavailable".to_string()),
            };
            coordinator
                .context_indexing
                .lock()
                .await
                .remove(&session_id);
            match outcome {
                Ok(true) => tracing::debug!(
                    session_id,
                    "background Session context index reconciliation completed"
                ),
                Ok(false) => tracing::warn!(
                    session_id,
                    "background Session context index reconciliation remained incomplete"
                ),
                Err(error) => tracing::warn!(
                    session_id,
                    %error,
                    "background Session context index reconciliation failed"
                ),
            }
        });
    }

    #[cfg(test)]
    async fn activate_for_test(
        &self,
        request: EnsureSessionRequest,
    ) -> Result<EnsureSessionOutcome, String> {
        self.activate(request, SessionActivationIntent::Ensure)
            .await
    }

    async fn lock_for(&self, session_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.session_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(session_id).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(session_id.to_string(), Arc::downgrade(&lock));
        lock
    }

    pub(super) async fn acquire_exclusive(
        &self,
        session_id: &str,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        self.lock_for(session_id).await.lock_owned().await
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

    async fn begin_hydration(
        &self,
        session_id: &str,
        manifest: &SessionRecoveryManifest,
    ) -> Result<(), String> {
        self.register_manifest_metadata(manifest, []).await;
        let requested = manifest.transcript_bytes;
        let budget = self.recovery.hot_bytes as u64;
        if requested > budget {
            return Err(format!(
                "session {session_id} requires {requested} hot bytes, exceeding the configured {budget}-byte working-set budget"
            ));
        }
        loop {
            let mut state = self.working_set.lock().await;
            let projected = state
                .hot_bytes
                .saturating_add(state.reserved_bytes)
                .saturating_add(requested);
            if projected <= budget {
                if let Some(entry) = state.entries.get_mut(session_id) {
                    entry.status = SessionHydrationStatus::Hydrating;
                    entry.last_error = None;
                }
                state.reserved_bytes = state.reserved_bytes.saturating_add(requested);
                state.concurrent_hydrations = state.concurrent_hydrations.saturating_add(1);
                return Ok(());
            }
            let mut candidates = state
                .entries
                .values()
                .filter(|entry| {
                    entry.session_id != session_id
                        && entry.status == SessionHydrationStatus::Ready
                        && entry.pin_reasons.is_empty()
                })
                .map(|entry| (entry.last_activity_ms, entry.session_id.clone()))
                .collect::<Vec<_>>();
            candidates.sort();
            drop(state);
            if candidates.is_empty() {
                return Err(format!(
                    "session {session_id} cannot reserve {requested} hot bytes because all remaining Runtime carriers are pinned"
                ));
            }
            let mut evicted = false;
            for (_, candidate) in &candidates {
                if self.try_evict_under_keyed_gate(candidate).await {
                    evicted = true;
                    break;
                }
            }
            if !evicted {
                return Err(format!(
                    "session {session_id} cannot reserve {requested} hot bytes because every eviction candidate is executing"
                ));
            }
        }
    }

    async fn try_evict_under_keyed_gate(&self, session_id: &str) -> bool {
        let lock = self.lock_for(session_id).await;
        let Ok(_guard) = lock.try_lock_owned() else {
            return false;
        };
        let victim_bytes = {
            let mut state = self.working_set.lock().await;
            let Some(entry) = state.entries.get(session_id) else {
                return false;
            };
            if entry.status != SessionHydrationStatus::Ready || !entry.pin_reasons.is_empty() {
                return false;
            }
            let bytes = entry.transcript_bytes;
            state.hot_bytes = state.hot_bytes.saturating_sub(bytes);
            state.evicting_bytes = state.evicting_bytes.saturating_add(bytes);
            if let Some(entry) = state.entries.get_mut(session_id) {
                entry.status = SessionHydrationStatus::MetadataLoaded;
                entry.last_error = None;
            }
            bytes
        };
        self.runtime
            .remove_active_runtime_if_present(session_id)
            .await;
        self.resource_lifecycle.unregister(session_id).await;
        let mut state = self.working_set.lock().await;
        state.evicting_bytes = state.evicting_bytes.saturating_sub(victim_bytes);
        drop(state);
        tracing::info!(
            session_id,
            "evicted unpinned hot Runtime carrier under keyed working-set gate"
        );
        true
    }

    async fn finish_hydration(
        &self,
        session_id: &str,
        actual_bytes: u64,
        result: Result<(), &str>,
    ) -> Result<(), String> {
        let mut state = self.working_set.lock().await;
        let previous = state.entries.get(session_id).map(|entry| entry.status);
        let reserved_bytes = state
            .entries
            .get(session_id)
            .map_or(0, |entry| entry.transcript_bytes);
        if previous == Some(SessionHydrationStatus::Hydrating) {
            state.reserved_bytes = state.reserved_bytes.saturating_sub(reserved_bytes);
            state.concurrent_hydrations = state.concurrent_hydrations.saturating_sub(1);
        }
        match result {
            Ok(()) => {
                let budget = self.recovery.hot_bytes as u64;
                let projected = state.hot_bytes.saturating_add(actual_bytes);
                if previous == Some(SessionHydrationStatus::Hydrating) && projected > budget {
                    let error = format!(
                        "session {session_id} hydrated to {actual_bytes} bytes and would exceed the {budget}-byte working-set budget"
                    );
                    if let Some(entry) = state.entries.get_mut(session_id) {
                        entry.status = SessionHydrationStatus::Degraded;
                        entry.transcript_bytes = actual_bytes;
                        entry.last_error = Some(error.clone());
                    }
                    return Err(error);
                }
                if previous != Some(SessionHydrationStatus::Ready) {
                    state.hot_bytes = state.hot_bytes.saturating_add(actual_bytes);
                }
                if let Some(entry) = state.entries.get_mut(session_id) {
                    entry.status = SessionHydrationStatus::Ready;
                    entry.transcript_bytes = actual_bytes;
                    entry.last_error = None;
                }
            }
            Err(error) => {
                if previous == Some(SessionHydrationStatus::Ready) {
                    state.hot_bytes = state.hot_bytes.saturating_sub(reserved_bytes);
                }
                if let Some(entry) = state.entries.get_mut(session_id) {
                    entry.status = SessionHydrationStatus::Degraded;
                    entry.last_error = Some(error.to_string());
                }
            }
        }
        Ok(())
    }

    async fn actual_hydration_bytes(&self, session_id: &str, fallback: u64) -> u64 {
        self.repository
            .stored_recovery_manifest(session_id)
            .await
            .ok()
            .flatten()
            .map_or(fallback, |manifest| manifest.transcript_bytes)
    }

    async fn mark_metadata_only(&self, session_id: &str) {
        let mut state = self.working_set.lock().await;
        let bytes = state
            .entries
            .get(session_id)
            .filter(|entry| entry.status == SessionHydrationStatus::Ready)
            .map_or(0, |entry| entry.transcript_bytes);
        let reserved = state
            .entries
            .get(session_id)
            .filter(|entry| entry.status == SessionHydrationStatus::Hydrating)
            .map_or(0, |entry| entry.transcript_bytes);
        state.hot_bytes = state.hot_bytes.saturating_sub(bytes);
        state.reserved_bytes = state.reserved_bytes.saturating_sub(reserved);
        if reserved > 0 {
            state.concurrent_hydrations = state.concurrent_hydrations.saturating_sub(1);
        }
        if let Some(entry) = state.entries.get_mut(session_id) {
            entry.status = SessionHydrationStatus::MetadataLoaded;
            entry.last_error = None;
        }
    }

    pub(crate) async fn working_set_projection(&self) -> SessionWorkingSetProjection {
        let state = self.working_set.lock().await;
        let mut projection = SessionWorkingSetProjection {
            hot_bytes: state.hot_bytes,
            reserved_bytes: state.reserved_bytes,
            evicting_bytes: state.evicting_bytes,
            byte_budget: self.recovery.hot_bytes as u64,
            concurrent_hydrations: state.concurrent_hydrations,
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
            let Ok(Some(mut manifest)) =
                self.repository.stored_recovery_manifest(&session_id).await
            else {
                continue;
            };
            let pending_approval = pending.contains(&session_id);
            if manifest.pending_approval != pending_approval {
                if let Ok(Some(updated)) = self
                    .repository
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
                    .repository
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
        let candidates = {
            let state = self.working_set.lock().await;
            let mut candidates = state
                .entries
                .values()
                .filter(|entry| {
                    entry.session_id != exclude
                        && entry.status == SessionHydrationStatus::Ready
                        && entry.pin_reasons.is_empty()
                })
                .map(|entry| (entry.last_activity_ms, entry.session_id.clone()))
                .collect::<Vec<_>>();
            candidates.sort();
            candidates
        };
        for (_, candidate) in candidates {
            if self.try_evict_under_keyed_gate(&candidate).await {
                return Some(candidate);
            }
        }
        None
    }

    pub(super) async fn activate(
        &self,
        request: EnsureSessionRequest,
        intent: SessionActivationIntent,
    ) -> Result<EnsureSessionOutcome, String> {
        let session_id = request.session_id.trim();
        if session_id.is_empty() {
            return Err("session id is required".to_string());
        }
        let lock = self.lock_for(session_id).await;
        let _guard = lock.lock().await;
        let session_repository = &self.repository;
        let mut existing = session_repository
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?;
        match (intent, existing.is_some()) {
            (SessionActivationIntent::CreateNew, true) => {
                return Err(format!("session {session_id} already exists"));
            }
            (SessionActivationIntent::ExistingOnly, false) => {
                return Err(format!("session {session_id} not found"));
            }
            _ => {}
        }
        if let Some(record) = existing.as_ref() {
            if is_terminal_session_status(&record.status) {
                return Err(format!(
                    "session {session_id} cannot be activated from terminal lifecycle state {}",
                    record.status
                ));
            }
        }
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
                    session_repository
                        .update_stored_session(record)
                        .await
                        .map_err(|error| error.to_string())?;
                }
            }
        }

        if self.runtime.has_active_session(session_id) {
            self.resource_lifecycle.register(session_id).await;
            self.resource_lifecycle.mark_active(session_id).await;
            let mut record = match existing {
                Some(record) => record,
                None => {
                    let record = self.persist_new_record(&request).await?;
                    if let Err(error) = self.presence_ledger.mark_active(session_id).await {
                        return Err(self
                            .rollback_created_session(&record, error.to_string())
                            .await);
                    }
                    record
                }
            };
            let manifest = self
                .repository
                .stored_recovery_manifest(session_id)
                .await
                .map_err(|error| error.to_string())?
                .unwrap_or_else(|| manifest_from_record(&record));
            self.register_manifest_metadata(&manifest, []).await;
            let actual_bytes = self
                .actual_hydration_bytes(session_id, manifest.transcript_bytes)
                .await;
            if let Err(error) = self
                .finish_hydration(session_id, actual_bytes, Ok(()))
                .await
            {
                self.runtime
                    .remove_active_runtime_if_present(session_id)
                    .await;
                self.resource_lifecycle.unregister(session_id).await;
                return Err(error);
            }
            let explicit_model = request
                .model
                .as_deref()
                .filter(|model| !model.trim().is_empty());
            let model = match explicit_model {
                Some(model) => self.runtime.resolve_session_model(Some(model))?,
                None => self
                    .runtime
                    .resolve_persisted_session_model(record.model.as_deref())?,
            };
            if record.model.as_deref() != Some(model.as_str()) {
                record.model = Some(model.clone());
                self.repository
                    .update_stored_session(&record)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            return Ok(EnsureSessionOutcome {
                session_id: session_id.to_string(),
                model,
                created: false,
                restored: false,
                record,
            });
        }

        if existing.is_none()
            && self.max_active_sessions.is_some_and(|maximum| {
                session_repository.list_active_session_ids().len() >= maximum
            })
        {
            self.refresh_working_set_signals().await;
            self.evict_one_unpinned_for_capacity(session_id)
                .await
                .ok_or_else(|| {
                    format!(
                        "active session limit {} reached and all hot Runtime carriers are pinned",
                        self.max_active_sessions.unwrap_or_default()
                    )
                })?;
        }

        let created = existing.is_none();
        let mut record = match existing {
            Some(record) => record,
            None => self.persist_new_record(&request).await?,
        };
        if !created
            && self
                .max_active_sessions
                .is_some_and(|maximum| self.repository.list_active_session_ids().len() >= maximum)
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
        let explicit_model = request
            .model
            .as_deref()
            .filter(|model| !model.trim().is_empty());
        let model = match explicit_model {
            Some(model) => self.runtime.resolve_session_model(Some(model))?,
            None => self
                .runtime
                .resolve_persisted_session_model(record.model.as_deref())?,
        };
        if record.model.as_deref() != Some(model.as_str()) {
            record.model = Some(model.clone());
            self.repository
                .update_stored_session(&record)
                .await
                .map_err(|error| error.to_string())?;
        }

        let manifest = self
            .repository
            .stored_recovery_manifest(session_id)
            .await
            .map_err(|error| error.to_string())?
            .unwrap_or_else(|| manifest_from_record(&record));
        if let Err(error) = self.begin_hydration(session_id, &manifest).await {
            if created {
                return Err(self.rollback_created_session(&record, error).await);
            }
            return Err(error);
        }
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
            let _ = self.finish_hydration(session_id, 0, Err(&error)).await;
            if created {
                return Err(self.rollback_created_session(&record, error).await);
            }
            return Err(error);
        }
        let actual_bytes = self
            .actual_hydration_bytes(session_id, manifest.transcript_bytes)
            .await;
        if let Err(error) = self
            .finish_hydration(session_id, actual_bytes, Ok(()))
            .await
        {
            self.runtime
                .remove_active_runtime_if_present(session_id)
                .await;
            if created {
                return Err(self.rollback_created_session(&record, error).await);
            }
            return Err(error);
        }
        if let Err(error) = self.register_lifecycle(session_id, !created).await {
            self.runtime
                .remove_active_runtime_if_present(session_id)
                .await;
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
        let model = self
            .runtime
            .resolve_session_model(request.model.as_deref())?;
        let record = SessionRecord {
            session_id: request.session_id.clone(),
            platform,
            chat_id: request
                .chat_id
                .clone()
                .unwrap_or_else(|| request.session_id.clone()),
            user_id: request.user_id.clone(),
            model: Some(model),
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
        self.repository
            .upsert_stored_session(&record)
            .await
            .map_err(|error| error.to_string())?;
        Ok(record)
    }

    async fn rollback_created_session(&self, record: &SessionRecord, cause: String) -> String {
        match self
            .repository
            .delete_stored_session(&record.session_id)
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
        self.presence_ledger
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
        self.unload_runtime_under_guard(session_id).await
    }

    pub(super) async fn unload_runtime_under_guard(&self, session_id: &str) -> bool {
        let removed = self
            .runtime
            .remove_active_runtime_if_present(session_id)
            .await;
        self.resource_lifecycle.unregister(session_id).await;
        self.mark_metadata_only(session_id).await;
        removed
    }

    pub(crate) async fn recover_active_sessions(&self) -> SessionRecoverySummary {
        self.recover_sessions(true).await
    }

    /// Restore only Sessions whose durable work cannot safely wait for an
    /// on-demand activation. Historical and merely warm Sessions stay as
    /// metadata until a surface opens them.
    pub(crate) async fn recover_required_sessions(&self) -> SessionRecoverySummary {
        self.recover_sessions(false).await
    }

    async fn recover_sessions(&self, include_warm_sessions: bool) -> SessionRecoverySummary {
        let mut summary = SessionRecoverySummary::default();
        let mut offset = 0usize;
        let mut manifests = Vec::new();
        let page_size = self.recovery.manifest_page_size;
        loop {
            let page_result = if include_warm_sessions {
                self.repository
                    .active_recovery_manifests(offset, page_size)
                    .await
            } else {
                self.repository
                    .required_recovery_manifests(offset, page_size)
                    .await
            };
            let page = match page_result {
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

        if !include_warm_sessions {
            let known = manifests
                .iter()
                .map(|manifest| manifest.session_id.as_str())
                .collect::<BTreeSet<_>>();
            let missing = pending_approval_sessions
                .union(&continuation_sessions)
                .filter(|session_id| !known.contains(session_id.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                match self
                    .repository
                    .stored_recovery_manifests_by_ids(&missing)
                    .await
                {
                    Ok(Some(additional)) => {
                        summary.discovered += additional.len();
                        manifests.extend(additional);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        summary.failed += 1;
                        summary.failures.push(format!(
                            "failed to recover derived Session manifests: {error}"
                        ));
                    }
                }
            }
        }

        let provider_snapshot = self.runtime.provider_registry().pin();
        let session_ids = manifests
            .iter()
            .map(|manifest| manifest.session_id.clone())
            .collect::<Vec<_>>();
        let mut records = match self.repository.stored_sessions_by_ids(&session_ids).await {
            Ok(Some(records)) => records
                .into_iter()
                .map(|record| (record.session_id.clone(), record))
                .collect::<BTreeMap<_, _>>(),
            Ok(None) => BTreeMap::new(),
            Err(error) => {
                summary.failed += 1;
                summary.failures.push(format!(
                    "failed to batch-load Session recovery metadata: {error}"
                ));
                return summary;
            }
        };
        let mut runtime_manifests = Vec::new();
        for manifest in &mut manifests {
            let record = match records.remove(&manifest.session_id) {
                Some(record) => record,
                None => {
                    summary.failed += 1;
                    summary.failures.push(format!(
                        "{}: recovery manifest has no Session record",
                        manifest.session_id
                    ));
                    continue;
                }
            };
            if is_terminal_session_status(&record.status) {
                tracing::warn!(
                    session_id = manifest.session_id,
                    status = record.status,
                    "Ignoring a stale recovery signal for a terminal Session"
                );
                continue;
            }
            let pending_approval = pending_approval_sessions.contains(&manifest.session_id);
            if manifest.pending_approval != pending_approval {
                match self
                    .repository
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
            let has_continuation = continuation_sessions.contains(&manifest.session_id);
            if has_continuation && !manifest.mission_agent_team_continuation {
                match self
                    .repository
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
            if is_internal_context_record(&record) {
                summary.metadata_only += 1;
                continue;
            }
            // A pending approval is durable control-plane state. It must remain
            // visible and pinned, but it does not require a model carrier or a
            // hydrated transcript unless the same Session also owns in-flight
            // execution that must resume.
            if manifest.pending_approval
                && !manifest.in_flight_turn
                && !manifest.mission_agent_team_continuation
            {
                summary.required += 1;
                summary.metadata_only += 1;
                continue;
            }
            let unconfigured_model = record
                .model
                .as_deref()
                .filter(|model| !model.trim().is_empty())
                .filter(|model| provider_snapshot.resolve(model).is_none());
            if let Some(model) = unconfigured_model {
                if self.runtime.configured_model().is_none() {
                    summary.model_rebind_required += 1;
                    tracing::warn!(
                        session_id = manifest.session_id,
                        model,
                        "Session metadata restored without a Runtime carrier because its model is not configured and no default model exists"
                    );
                    continue;
                }
                tracing::info!(
                    session_id = manifest.session_id,
                    model,
                    "Session Runtime recovery will rebind an unconfigured persisted model to the configured default"
                );
            }
            runtime_manifests.push(manifest.clone());
        }

        let now_ms = current_time_ms();
        let recent_cutoff = now_ms.saturating_sub(self.recovery.recent_window_ms);
        let mut required = Vec::new();
        let mut attached = Vec::new();
        let mut recent = Vec::new();
        for manifest in runtime_manifests {
            if manifest.in_flight_turn || manifest.mission_agent_team_continuation {
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
        let mut selected_attached = Vec::new();
        let mut selected_recent = Vec::new();
        if include_warm_sessions {
            let mut attached_bytes = 0u64;
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
            for manifest in recent {
                let projected = recent_bytes.saturating_add(manifest.transcript_bytes);
                if projected > self.recovery.recent_bytes as u64 {
                    continue;
                }
                recent_bytes = projected;
                summary.recent += 1;
                selected_recent.push(manifest);
            }
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
                let result = self
                    .activate(request, SessionActivationIntent::ExistingOnly)
                    .await;
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

    #[must_use]
    pub(super) fn repository(&self) -> Arc<SessionRepository> {
        Arc::clone(&self.repository)
    }

    #[must_use]
    pub(super) fn presence_ledger(&self) -> Arc<SessionPresenceLedger> {
        Arc::clone(&self.presence_ledger)
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn is_internal_context_record(record: &SessionRecord) -> bool {
    record
        .metadata_json
        .as_deref()
        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
        .and_then(|metadata| {
            metadata
                .get("internal_context")
                .and_then(serde_json::Value::as_bool)
        })
        .unwrap_or(false)
}

fn is_terminal_session_status(status: &str) -> bool {
    matches!(status, "archiving" | "archived" | "deleting" | "deleted")
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
        event_cursor: 0,
        history_revision: 0,
        transcript_messages: record.message_count.max(0) as u64,
        transcript_bytes: 0,
        latest_checkpoint_sequence: None,
        latest_checkpoint_event_id: None,
        index_generation: 0,
        indexed_through_sequence: None,
        index_card_count: 0,
        index_pending: record.message_count > 0,
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
    use crate::event_bus::SessionProjectionHub;
    use crate::gateway::HotSessionPool;
    use crate::services::session_service::presence::SessionPresenceLedger;
    use crate::services::session_service::repository::SessionRepository;
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
                        models: vec![
                            crate::DEFAULT_MODEL_ALIAS.to_string(),
                            "test-model".to_string(),
                        ],
                        protocol: Some("completions".to_string()),
                        parallel_tool_calls: Default::default(),
                        early_tool_start: Default::default(),
                    },
                )]),
            })
            .expect("valid inert test provider registry"),
        )
    }

    fn test_manager(
        max_active_sessions: usize,
    ) -> (
        Arc<SessionActivationCoordinator>,
        Arc<session::UnifiedSessionStore>,
        Arc<HotSessionPool>,
        Arc<SessionWorkingSetManager>,
    ) {
        test_manager_with_limits(max_active_sessions, max_active_sessions)
    }

    fn test_manager_with_limits(
        runtime_max_active_sessions: usize,
        manager_max_active_sessions: usize,
    ) -> (
        Arc<SessionActivationCoordinator>,
        Arc<session::UnifiedSessionStore>,
        Arc<HotSessionPool>,
        Arc<SessionWorkingSetManager>,
    ) {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let active = Arc::new(HotSessionPool::with_max_sessions(
            runtime_max_active_sessions,
        ));
        let event_bus = SessionProjectionHub::new();
        let session_repository = Arc::new(SessionRepository::new(
            Arc::clone(&active),
            Some(Arc::clone(&store)),
            Arc::clone(&event_bus),
        ));
        let presence_ledger = Arc::new(SessionPresenceLedger::with_store(Arc::clone(&store)));
        let runtime_services = runtime::RuntimeServices::in_memory().unwrap();
        let session_runtime_port =
            crate::session_runtime_data_port::GatewaySessionRuntimePort::new_for_test(
                Arc::clone(&session_repository),
                Arc::clone(&presence_ledger),
            );
        runtime_services
            .install_session_ports(
                session_runtime_port.clone(),
                session_runtime_port.clone(),
                session_runtime_port.clone(),
            )
            .unwrap();
        let runtime = Arc::new(
            RuntimeService::new(
                Arc::clone(&active),
                Arc::new(session::SessionLeaseRegistry::default()),
                session_runtime_port,
                event_bus,
                std::time::Instant::now(),
                Some("test-model".to_string()),
                test_provider_registry(),
                Arc::new(runtime::UpgradeCoordinator::new()),
                runtime_services,
            )
            .unwrap(),
        );
        let resource_lifecycle = Arc::new(SessionWorkingSetManager::new(
            runtime::session_lifecycle::SessionLifecycleConfig::default(),
        ));
        let recovery = runtime::SessionRecoveryConfig {
            recent_window_ms: 0,
            ..runtime::SessionRecoveryConfig::default()
        };
        (
            Arc::new(SessionActivationCoordinator::new(
                runtime,
                session_repository,
                presence_ledger,
                Arc::clone(&resource_lifecycle),
                Some(manager_max_active_sessions),
                recovery,
            )),
            store,
            active,
            resource_lifecycle,
        )
    }

    #[tokio::test]
    async fn activation_creates_hot_durable_and_lifecycle_state() {
        let (manager, store, active, lifecycle) = test_manager(8);
        let outcome = manager
            .activate_for_test(EnsureSessionRequest::new(
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
                .presence_ledger()
                .snapshot("session-new")
                .await
                .unwrap()
                .state,
            session::SessionLifecycleState::Active
        );
    }

    #[tokio::test]
    async fn concurrent_surface_activation_creates_exactly_one_session() {
        let (manager, store, active, _) = test_manager(32);
        let mut tasks = Vec::new();
        for _ in 0..20 {
            let manager = Arc::clone(&manager);
            tasks.push(tokio::spawn(async move {
                manager
                    .activate_for_test(EnsureSessionRequest::new(
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
        manager.activate_for_test(owner_request).await.unwrap();
        assert!(manager.unload_runtime("session-owned").await);
        assert!(active.get("session-owned").is_none());
        assert!(lifecycle.check_session("session-owned").await.is_none());

        let mut foreign_request =
            EnsureSessionRequest::new("session-owned", None, SessionSource::MissionControl);
        foreign_request.owner_principal_id = Some("principal-b".to_string());
        let error = manager
            .activate_for_test(foreign_request)
            .await
            .unwrap_err();

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
            .activate_for_test(EnsureSessionRequest::new(
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
        let error = manager.activate_for_test(ordinary).await.unwrap_err();
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
            .activate_for_test(privileged)
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
    async fn activation_failure_compensates_session_lifecycle() {
        let (manager, store, active, _) = test_manager_with_limits(1, 2);
        manager
            .activate_for_test(EnsureSessionRequest::new(
                "session-capacity-owner",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap();

        let error = manager
            .activate_for_test(EnsureSessionRequest::new(
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
    }

    #[tokio::test]
    async fn hot_surface_activation_averages_below_five_milliseconds() {
        let (manager, _, _, _) = test_manager(8);
        manager
            .activate_for_test(EnsureSessionRequest::new(
                "session-hot",
                None,
                SessionSource::Tui,
            ))
            .await
            .unwrap();
        let started = std::time::Instant::now();
        for _ in 0..100 {
            let outcome = manager
                .activate_for_test(EnsureSessionRequest::new(
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
            "100 hot surface activations took {:?}",
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
            .activate_for_test(EnsureSessionRequest::new(
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
            .activate_for_test(EnsureSessionRequest::new(
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
    async fn capacity_limit_evicts_only_hot_state_and_never_blocks_durable_recovery() {
        let (manager, store, active, lifecycle) = test_manager(1);
        manager
            .activate_for_test(EnsureSessionRequest::new(
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
            .activate_for_test(EnsureSessionRequest::new(
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
            .activate_for_test(EnsureSessionRequest::new(
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
            .activate_for_test(EnsureSessionRequest::new(
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
        let lifecycle = Arc::new(SessionWorkingSetManager::new(
            runtime::session_lifecycle::SessionLifecycleConfig {
                idle_timeout: Some(std::time::Duration::from_millis(10)),
                max_ttl: None,
                max_active_sessions: Some(8),
                eviction_policy: runtime::session_lifecycle::EvictionPolicy::Lru,
                cleanup_interval: std::time::Duration::from_millis(5),
            },
        ));
        let manager = SessionActivationCoordinator::new(
            base.runtime().clone(),
            Arc::clone(&base.repository),
            Arc::clone(&base.presence_ledger),
            lifecycle,
            Some(8),
            runtime::SessionRecoveryConfig::default(),
        );
        manager
            .activate_for_test(EnsureSessionRequest::new(
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
        let lifecycle = Arc::new(SessionWorkingSetManager::new(
            runtime::session_lifecycle::SessionLifecycleConfig {
                idle_timeout: Some(std::time::Duration::from_millis(10)),
                max_ttl: None,
                max_active_sessions: Some(8),
                eviction_policy: runtime::session_lifecycle::EvictionPolicy::Lru,
                cleanup_interval: std::time::Duration::from_millis(5),
            },
        ));
        let manager = SessionActivationCoordinator::new(
            base.runtime().clone(),
            Arc::clone(&base.repository),
            Arc::clone(&base.presence_ledger),
            lifecycle,
            Some(8),
            runtime::SessionRecoveryConfig::default(),
        );
        manager
            .activate_for_test(EnsureSessionRequest::new(
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
    async fn startup_recovery_keeps_internal_and_unconfigured_models_out_of_runtime() {
        let (manager, store, active, _) = test_manager(16);
        for record in [
            SessionRecord {
                session_id: "internal-context".to_string(),
                platform: "internal".to_string(),
                chat_id: "internal-context".to_string(),
                user_id: None,
                model: None,
                created_at: Utc::now().to_rfc3339(),
                last_activity: Utc::now().to_rfc3339(),
                message_count: 0,
                reset_policy: "none".to_string(),
                metadata_json: Some(r#"{"internal_context":true}"#.to_string()),
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            },
            SessionRecord {
                session_id: "legacy-provider-session".to_string(),
                platform: "tui".to_string(),
                chat_id: "legacy-provider-session".to_string(),
                user_id: None,
                model: Some("claude-legacy-default".to_string()),
                created_at: Utc::now().to_rfc3339(),
                last_activity: "2999-01-01T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            },
        ] {
            store.upsert_session(&record).await.unwrap();
        }

        let summary = manager.recover_active_sessions().await;

        assert_eq!(summary.discovered, 2);
        assert_eq!(summary.metadata_loaded, 2);
        assert_eq!(summary.metadata_only, 1);
        assert_eq!(summary.model_rebind_required, 0);
        assert_eq!(summary.recovered, 1);
        assert_eq!(summary.failed, 0, "{:?}", summary.failures);
        assert_eq!(active.list(), vec!["legacy-provider-session".to_string()]);
        assert_eq!(manager.runtime().hydration_stats().attempts, 1);
        assert_eq!(
            store
                .get_session("legacy-provider-session")
                .await
                .unwrap()
                .unwrap()
                .model
                .as_deref(),
            Some("test-model")
        );
    }

    #[test]
    fn runtime_model_resolution_requires_configured_provider_membership() {
        let (manager, _, _, _) = test_manager(8);

        assert_eq!(
            manager.runtime().resolve_session_model(None).unwrap(),
            "test-model"
        );
        assert_eq!(
            manager
                .runtime()
                .resolve_session_model(Some("test-model"))
                .unwrap(),
            "test-model"
        );
        assert_eq!(
            manager
                .runtime()
                .resolve_persisted_session_model(Some("claude-legacy-default"))
                .unwrap(),
            "test-model"
        );
        assert!(manager
            .runtime()
            .resolve_session_model(Some("claude-implicit"))
            .unwrap_err()
            .contains("not declared by any configured provider"));
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
                &session::SessionRuntimeOutboxRequest {
                    input_id: "input-required".to_string(),
                    request_id: "request-required".to_string(),
                    turn_id: "turn-required".to_string(),
                    message_id: "message-required".to_string(),
                    session_generation: 1,
                    decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
                    target_turn_id: None,
                    classification_json: None,
                    task_route_hint: None,
                    created_at_ms: 1,
                    runtime_options_json: None,
                },
            )
            .await
            .unwrap();

        let summary = manager.recover_required_sessions().await;
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
    async fn startup_recovery_reconciles_pending_approval_without_runtime_hydration() {
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
        let source = runtime::ApprovalSource {
            kind: runtime::ApprovalSourceKind::Session,
            session_id: Some("session-approval".to_string()),
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: None,
            review_ref: None,
            application: None,
        };
        manager
            .runtime()
            .runtime_services()
            .approval_queue()
            .submit(runtime::SubmitGlobalApprovalRequest {
                context: harness_contract::policy::ApprovalContext::owned(
                    &source,
                    "write",
                    "workspace:session-approval",
                ),
                source,
                action: "write".to_string(),
                summary: "approval required".to_string(),
                risk: harness_contract::core::TaskRisk::High,
                evidence_refs: Vec::new(),
                timeout_policy: runtime::ApprovalTimeoutPolicy::Pending,
            })
            .unwrap();

        let summary = manager.recover_required_sessions().await;
        assert_eq!(summary.required, 1);
        assert_eq!(summary.recovered, 0);
        assert_eq!(summary.metadata_only, 1);
        assert!(active.get("session-approval").is_none());
        let stats = manager.runtime().hydration_stats();
        assert_eq!(stats.attempts, 0);
        assert_eq!(stats.body_reads, 0);
        let manifest = store
            .get_session_recovery_manifest("session-approval")
            .await
            .unwrap()
            .unwrap();
        assert!(manifest.pending_approval);
    }

    #[tokio::test]
    async fn startup_recovery_ignores_terminal_session_with_stale_approval_signal() {
        let (manager, store, active, _) = test_manager(16);
        store
            .upsert_session(&SessionRecord {
                session_id: "session-deleted-approval".to_string(),
                platform: "webui".to_string(),
                chat_id: "session-deleted-approval".to_string(),
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
                status: "deleted".to_string(),
            })
            .await
            .unwrap();
        let source = runtime::ApprovalSource {
            kind: runtime::ApprovalSourceKind::Session,
            session_id: Some("session-deleted-approval".to_string()),
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: None,
            review_ref: None,
            application: None,
        };
        manager
            .runtime()
            .runtime_services()
            .approval_queue()
            .submit(runtime::SubmitGlobalApprovalRequest {
                context: harness_contract::policy::ApprovalContext::owned(
                    &source,
                    "write",
                    "workspace:session-deleted-approval",
                ),
                source,
                action: "write".to_string(),
                summary: "stale approval".to_string(),
                risk: harness_contract::core::TaskRisk::High,
                evidence_refs: Vec::new(),
                timeout_policy: runtime::ApprovalTimeoutPolicy::Pending,
            })
            .unwrap();

        let summary = manager.recover_required_sessions().await;

        assert_eq!(summary.discovered, 1);
        assert_eq!(summary.required, 0);
        assert_eq!(summary.recovered, 0);
        assert_eq!(summary.metadata_loaded, 0);
        assert_eq!(summary.failed, 0, "{:?}", summary.failures);
        assert!(active.get("session-deleted-approval").is_none());
        assert!(manager.working_set_projection().await.entries.is_empty());
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
            .presence_ledger()
            .attach(
                "session-attached",
                session::SessionActor::new("web:test", "webui"),
            )
            .await
            .unwrap();
        store
            .set_session_recovery_signal(
                "session-mission",
                SessionRecoverySignal::MissionAgentTeamContinuation,
                true,
                1,
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
            .insert_message(&session::SessionMessage {
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
                    .activate_for_test(EnsureSessionRequest::new(
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
    async fn long_session_activation_reads_only_configured_tail() {
        let (manager, store, active, _) = test_manager(8);
        let session_id = "session-bounded-activation";
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
        let messages = (0..5_000)
            .map(|sequence| session::SessionMessage {
                stable_message_id: format!("bounded-{sequence}"),
                session_id: session_id.to_string(),
                sequence,
                role: if sequence % 2 == 0 {
                    "user"
                } else {
                    "assistant"
                }
                .to_string(),
                content_json: serde_json::json!([
                    {"type":"text","text":format!("durable history payload {sequence}")}
                ])
                .to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: sequence as u64,
            })
            .collect::<Vec<_>>();
        let all_bytes = messages
            .iter()
            .map(|message| {
                message.stable_message_id.len()
                    + message.session_id.len()
                    + message.role.len()
                    + message.content_json.len()
            })
            .sum::<usize>();
        store.insert_messages_batch(&messages).await.unwrap();

        manager
            .activate_for_test(EnsureSessionRequest::new(
                session_id,
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap();

        assert!(active.get(session_id).is_some());
        assert_eq!(store.get_message_count(session_id).await.unwrap(), 5_000);
        let stats = manager.runtime().hydration_stats();
        assert_eq!(stats.attempts, 1);
        assert_eq!(stats.body_reads, 1);
        assert!(
            stats.body_bytes < (all_bytes / 10) as u64,
            "activation read {} bytes from {} durable bytes",
            stats.body_bytes,
            all_bytes
        );
    }

    #[tokio::test]
    async fn byte_budget_evicts_unpinned_hot_runtime_but_keeps_durable_history() {
        let (base, store, active, _) = test_manager(8);
        let lifecycle = Arc::new(SessionWorkingSetManager::default());
        let manager = SessionActivationCoordinator::new(
            base.runtime().clone(),
            Arc::clone(&base.repository),
            Arc::clone(&base.presence_ledger),
            Arc::clone(&lifecycle),
            Some(8),
            runtime::SessionRecoveryConfig {
                // Each fixture transcript is currently 74 bytes. Keep enough
                // room for exactly one hot carrier so the second activation
                // proves eviction rather than impossible admission.
                hot_bytes: 100,
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
                .insert_message(&session::SessionMessage {
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
            .activate_for_test(EnsureSessionRequest::new(
                "session-byte-a",
                None,
                SessionSource::WebUi,
            ))
            .await
            .unwrap();
        manager
            .activate_for_test(EnsureSessionRequest::new(
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
    async fn cold_runtime_is_activated_before_one_way_runtime_execution() {
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
        manager
            .activate_for_test(EnsureSessionRequest::new(
                &record.session_id,
                None,
                SessionSource::Internal,
            ))
            .await
            .unwrap();
        let ingress = session::SessionRuntimeOutboxRecord {
            input_id: "cold-input".to_string(),
            request_id: "cold-request".to_string(),
            turn_id: "cold-turn".to_string(),
            message_id: "cold-message".to_string(),
            session_id: record.session_id.clone(),
            sequence: 0,
            session_generation: 1,
            decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            task_route_hint: None,
            status: session::SessionRuntimeInputStatus::Claimed,
            runtime_commit_cursor: None,
            attempts: 1,
            next_attempt_at_ms: 0,
            claim_owner: Some("test-worker".to_string()),
            claim_token: Some("cold-claim".to_string()),
            claim_fence_epoch: Some(1),
            claim_expires_at_ms: Some(u64::MAX),
            failure_class: None,
            last_error: None,
            revision: 1,
            created_at_ms: 1,
            updated_at_ms: 1,
            terminal_at_ms: None,
            runtime_options_json: None,
        };

        let _ = manager
            .runtime()
            .execute_ingress_record(&ingress, "activation proof")
            .await;
        assert!(active.get(&record.session_id).is_some());
        assert_eq!(
            manager
                .presence_ledger
                .snapshot(&record.session_id)
                .await
                .unwrap()
                .state,
            session::SessionLifecycleState::Active
        );
    }

    #[tokio::test]
    async fn keyed_session_lock_registry_reclaims_completed_keys() {
        let (manager, _, _, _) = test_manager(8);
        {
            let _guard = manager.acquire_exclusive("session-lock-a").await;
            assert_eq!(manager.session_locks.lock().await.len(), 1);
        }
        {
            let _guard = manager.acquire_exclusive("session-lock-b").await;
            let locks = manager.session_locks.lock().await;
            assert_eq!(locks.len(), 1);
            assert!(locks.contains_key("session-lock-b"));
        }
    }
}
