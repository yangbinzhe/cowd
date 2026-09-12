//! Runtime-owned, execution-correlated live lifecycle projection.
//!
//! The durable event ledger stores immutable snapshots while this small cache
//! serves hot reads. Gateway may call the public RuntimeServices facade but
//! never owns transitions, revisions, metrics, or terminal state.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use harness_contract::context::ContextTurnReport;
use harness_contract::projection::{
    ContextUsageProjection, ExecutionLatencyProjection, ExecutionLiveOutputPart,
    ExecutionLiveState, ExecutionLiveStatus, RunMetricsProjection, SessionExecutionEntryProjection,
    SessionExecutionIndexProjection,
};
use serde::{Deserialize, Serialize};

use crate::execution_core::hot_state::{HotResidentClass, RuntimeHotStatePlane};
use crate::runtime_event_store::{DurableRuntimeEvent, RuntimeProjectionCheckpoint};
use crate::{CowdEvent, RuntimeEventStore};

const LIVE_PROJECTION_PREFIX: &str = "execution-live:";
// Keep enough canonical live text to repair a saturated Surface stream while
// remaining bounded per active execution. The byte offset carried alongside
// the snapshot makes truncation explicit instead of silently presenting a
// suffix as a complete answer.
const LIVE_OUTPUT_PREVIEW_LIMIT: usize = 1024 * 1024;

fn trim_live_preview(preview: &mut String) {
    if preview.len() <= LIVE_OUTPUT_PREVIEW_LIMIT {
        return;
    }
    let split = preview.len().saturating_sub(LIVE_OUTPUT_PREVIEW_LIMIT);
    let boundary = preview
        .char_indices()
        .find_map(|(index, _)| (index >= split).then_some(index))
        .unwrap_or(0);
    *preview = preview[boundary..].to_string();
}

fn live_projection_id(execution_id: &str) -> String {
    format!("{LIVE_PROJECTION_PREFIX}{execution_id}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveExecutionRecord {
    session_id: String,
    execution_id: String,
    /// Child Agent executions remain directly queryable, but they are not
    /// independent Session turns and must never enter the Session discovery
    /// index as roots.
    #[serde(default)]
    parent_execution_id: Option<String>,
    #[serde(default)]
    graph_id: Option<String>,
    live: ExecutionLiveState,
    #[serde(default)]
    tool_ids: BTreeSet<String>,
    #[serde(default)]
    approval_keys: BTreeSet<String>,
    #[serde(default)]
    file_touch_keys: BTreeSet<String>,
    #[serde(default)]
    context_item_ids: BTreeSet<String>,
    #[serde(default)]
    memory_item_ids: BTreeSet<String>,
    #[serde(default)]
    memory_evidence_ids: BTreeSet<String>,
    #[serde(default)]
    reality_item_ids: BTreeSet<String>,
    #[serde(default)]
    own_model_usage: Option<crate::RunModelTelemetry>,
    #[serde(default)]
    descendant_model_usage: BTreeMap<String, crate::RunModelTelemetry>,
    /// Monotonic request-local execution bus generation that most recently
    /// contributed model usage. Generation zero is the legacy/asynchronous
    /// surface relay and cannot overwrite a newer synchronous owner.
    #[serde(default)]
    model_usage_generation: u64,
    /// Sealed with every terminal winner. Once present, delayed child or
    /// relay telemetry cannot mutate the billed terminal truth.
    #[serde(default)]
    model_usage_fence_generation: Option<u64>,
    /// Reversible arbitration slot shared by Session terminal delivery and
    /// cancellation. Preparing a claim freezes competing terminal writers but
    /// deliberately leaves the public live status non-terminal until the
    /// corresponding durable business carrier commits.
    #[serde(default)]
    pending_terminal_claim: Option<PendingLiveTerminalClaim>,
    /// 30-minute warning buckets already surfaced for this execution. While a
    /// healthy execution keeps progressing it is never cut by a wall-clock
    /// deadline; instead the user is warned every 30 minutes and may choose to
    /// continue or finalize (collect intermediate results and produce the
    /// requested artifact).
    #[serde(default)]
    warning_buckets: u64,
}

#[derive(Debug, Clone, Copy)]
struct DurableLiveCheckpoint {
    source_cursor: u64,
    row_revision: u64,
    live_revision: u64,
    model_usage_generation: u64,
    model_usage_fence_generation: Option<u64>,
    updated_at_ms: u64,
}

impl LiveExecutionRecord {
    fn new(session_id: String, execution_id: String, turn_id: String) -> Self {
        let now = current_time_ms();
        Self {
            session_id,
            execution_id,
            parent_execution_id: None,
            graph_id: None,
            live: ExecutionLiveState {
                revision: 1,
                status: ExecutionLiveStatus::Queued,
                status_detail: Some("session input accepted".to_string()),
                turn_id: Some(turn_id),
                started_at_ms: now,
                updated_at_ms: now,
                last_progress_at_ms: now,
                context_usage: None,
                metrics: RunMetricsProjection::default(),
                latency: ExecutionLatencyProjection::default(),
                output_preview: None,
                output_preview_start_bytes: 0,
                output_bytes: 0,
                output_parts: Vec::new(),
                terminal_ref: None,
                error: None,
            },
            tool_ids: BTreeSet::new(),
            approval_keys: BTreeSet::new(),
            file_touch_keys: BTreeSet::new(),
            context_item_ids: BTreeSet::new(),
            memory_item_ids: BTreeSet::new(),
            memory_evidence_ids: BTreeSet::new(),
            reality_item_ids: BTreeSet::new(),
            own_model_usage: None,
            descendant_model_usage: BTreeMap::new(),
            model_usage_generation: 0,
            model_usage_fence_generation: None,
            pending_terminal_claim: None,
            warning_buckets: 0,
        }
    }

    fn admit_model_usage(&mut self, generation: u64) -> bool {
        if self.model_usage_fence_generation.is_some() || self.live.status.is_terminal() {
            tracing::debug!(
                execution_id = %self.execution_id,
                generation,
                fence_generation = ?self.model_usage_fence_generation,
                "ignored model telemetry after terminal usage fence"
            );
            return false;
        }
        if generation < self.model_usage_generation {
            tracing::debug!(
                execution_id = %self.execution_id,
                generation,
                current_generation = self.model_usage_generation,
                "ignored stale model telemetry generation"
            );
            return false;
        }
        self.model_usage_generation = generation;
        true
    }

    fn seal_model_usage(&mut self) {
        self.model_usage_fence_generation = Some(self.model_usage_generation);
    }

    fn refresh_model_usage_metrics(&mut self) {
        let mut input_tokens = 0_u64;
        let mut output_tokens = 0_u64;
        let mut cache_creation_input_tokens = 0_u64;
        let mut cache_read_input_tokens = 0_u64;
        let mut usage_count = 0_u64;
        let mut cache_dimensions_known = true;
        for telemetry in self
            .own_model_usage
            .iter()
            .chain(self.descendant_model_usage.values())
        {
            usage_count = usage_count.saturating_add(1);
            cache_dimensions_known &= telemetry.cache_dimensions_known;
            input_tokens = input_tokens.saturating_add(telemetry.input_tokens);
            output_tokens = output_tokens.saturating_add(telemetry.output_tokens);
            cache_creation_input_tokens =
                cache_creation_input_tokens.saturating_add(telemetry.cache_create_tokens);
            cache_read_input_tokens =
                cache_read_input_tokens.saturating_add(telemetry.cache_read_tokens);
        }
        self.live.metrics.input_tokens = input_tokens;
        self.live.metrics.output_tokens = output_tokens;
        self.live.metrics.cache_creation_input_tokens = cache_creation_input_tokens;
        self.live.metrics.cache_read_input_tokens = cache_read_input_tokens;
        self.live.metrics.cache_dimensions_known = usage_count > 0 && cache_dimensions_known;
        self.live.metrics.total_tokens = input_tokens
            .saturating_add(output_tokens)
            .saturating_add(cache_creation_input_tokens)
            .saturating_add(cache_read_input_tokens);
        self.refresh_latency();
    }

    fn refresh_latency(&mut self) {
        let observed_elapsed_ms = self
            .live
            .updated_at_ms
            .saturating_sub(self.live.started_at_ms);
        let provider_wall_ms = self
            .own_model_usage
            .as_ref()
            .map_or(0, |telemetry| telemetry.wall_duration_ms);
        let total_elapsed_ms = observed_elapsed_ms.max(provider_wall_ms);
        self.live.latency = ExecutionLatencyProjection {
            total_elapsed_ms,
            harness_elapsed_ms: total_elapsed_ms.saturating_sub(provider_wall_ms),
            provider_wall_ms,
            first_token_latency_ms: self
                .own_model_usage
                .as_ref()
                .and_then(|telemetry| telemetry.first_token_latency_ms),
            provider_active_stream_ms: self
                .own_model_usage
                .as_ref()
                .and_then(|telemetry| telemetry.active_stream_duration_ms)
                .unwrap_or_default()
                .min(provider_wall_ms),
        };
    }

    fn transition(&mut self, status: ExecutionLiveStatus, detail: Option<String>) -> bool {
        if self.live.status == status && self.live.status_detail == detail {
            return false;
        }
        if !allows_transition(self.live.status, status) {
            if is_terminal_live_status(self.live.status) && !is_terminal_live_status(status) {
                tracing::debug!(
                    execution_id = %self.execution_id,
                    terminal = ?self.live.status,
                    stale = ?status,
                    "absorbed stale Runtime live phase after terminal commit"
                );
            } else {
                tracing::warn!(
                    execution_id = %self.execution_id,
                    from = ?self.live.status,
                    to = ?status,
                    "ignored invalid Runtime live execution status transition"
                );
            }
            return false;
        }
        let now = current_time_ms();
        self.live.status = status;
        self.live.status_detail = detail;
        self.live.updated_at_ms = now;
        self.live.last_progress_at_ms = now;
        self.live.revision = self.live.revision.saturating_add(1);
        self.refresh_latency();
        true
    }

    fn touch(&mut self) {
        let now = current_time_ms();
        self.live.updated_at_ms = now;
        self.live.last_progress_at_ms = now;
        self.live.revision = self.live.revision.saturating_add(1);
        self.refresh_latency();
    }

    fn append_preview(&mut self, identity: &crate::CausalItemIdentity, text: &str) {
        let part = if let Some(index) = self
            .live
            .output_parts
            .iter()
            .position(|part| part.part_id == identity.segment_id)
        {
            &mut self.live.output_parts[index]
        } else {
            let index = self.live.output_parts.len();
            self.live.output_parts.push(ExecutionLiveOutputPart {
                model_step_id: identity.model_step_id.clone(),
                item_id: identity.item_id.clone(),
                part_id: identity.segment_id.clone(),
                causal_sequence: identity.causal_sequence,
                completed: false,
                preview: None,
                preview_start_bytes: 0,
                bytes: 0,
            });
            &mut self.live.output_parts[index]
        };
        part.bytes = part
            .bytes
            .saturating_add(u64::try_from(text.len()).unwrap_or(u64::MAX));
        let mut part_preview = part.preview.take().unwrap_or_default();
        part_preview.push_str(text);
        trim_live_preview(&mut part_preview);
        part.preview_start_bytes = part
            .bytes
            .saturating_sub(u64::try_from(part_preview.len()).unwrap_or(u64::MAX));
        part.preview = (!part_preview.is_empty()).then_some(part_preview);
        self.live
            .output_parts
            .sort_by_key(|part| part.causal_sequence);

        self.live.output_bytes = self
            .live
            .output_bytes
            .saturating_add(u64::try_from(text.len()).unwrap_or(u64::MAX));
        let mut preview = self.live.output_preview.take().unwrap_or_default();
        preview.push_str(text);
        trim_live_preview(&mut preview);
        self.live.output_preview_start_bytes = self
            .live
            .output_bytes
            .saturating_sub(u64::try_from(preview.len()).unwrap_or(u64::MAX));
        self.live.output_preview = (!preview.is_empty()).then_some(preview);
        self.touch();
    }

    fn complete_output_part(&mut self, identity: &crate::CausalItemIdentity) -> bool {
        let Some(part) = self
            .live
            .output_parts
            .iter_mut()
            .find(|part| part.part_id == identity.segment_id)
        else {
            return false;
        };
        if part.completed {
            return false;
        }
        part.completed = true;
        self.touch();
        true
    }

    fn apply_terminal_projection(
        &mut self,
        report: &ContextTurnReport,
        write_attempt_paths: &[String],
        terminal_ref: String,
    ) {
        let previous_context = self.live.context_usage.take();
        self.live.context_usage = context_usage_from_report(report).map(|mut completed| {
            if let Some(previous) = previous_context.as_ref() {
                if completed.model.is_none() {
                    completed.model = previous.model.clone();
                }
                // The turn ledger's `max_tokens` is the governed subsystem
                // budget, not the selected provider model's context window.
                // ProviderAttempt/ContextWindow already projected the latter;
                // keep that authority through terminal materialization while
                // still taking provider-calibrated input and component
                // attribution from the completed ledger.
                if previous.window_tokens.is_some() {
                    completed.window_tokens = previous.window_tokens;
                    completed.window_source = previous.window_source.clone();
                }
            }
            update_usage_percent(&mut completed);
            completed
        });
        if self.live.context_usage.is_none() {
            self.live.context_usage = previous_context;
        }
        self.live.metrics.tool_calls = self
            .live
            .metrics
            .tool_calls
            .max(report.observations.len() as u64);
        self.live.metrics.memory_recalls = self.live.metrics.memory_recalls.max(
            report
                .ledger
                .as_ref()
                .map(|ledger| {
                    ledger
                        .components
                        .iter()
                        .filter(|component| component.kind.eq_ignore_ascii_case("memory"))
                        .map(|component| component.occurrences)
                        .sum()
                })
                .unwrap_or_default(),
        );
        self.live.metrics.context_items =
            self.live
                .metrics
                .context_items
                .max(self.live.context_usage.as_ref().map_or(0, |usage| {
                    usage
                        .components
                        .iter()
                        .map(|component| component.occurrences)
                        .sum()
                }));
        for path in write_attempt_paths {
            self.file_touch_keys.insert(path.clone());
        }
        self.live.metrics.files_touched = self.file_touch_keys.len().try_into().unwrap_or(u64::MAX);
        self.live.terminal_ref = Some(terminal_ref);
    }

    fn complete(
        &mut self,
        report: &ContextTurnReport,
        write_attempt_paths: &[String],
        terminal_ref: String,
    ) -> bool {
        if self.live.status == ExecutionLiveStatus::Complete
            && self.live.terminal_ref.as_deref() == Some(terminal_ref.as_str())
        {
            self.apply_terminal_projection(report, write_attempt_paths, terminal_ref);
            self.touch();
            self.seal_model_usage();
            return true;
        }
        if !self.transition(
            ExecutionLiveStatus::Complete,
            Some("terminal committed".to_string()),
        ) {
            return false;
        }
        self.apply_terminal_projection(report, write_attempt_paths, terminal_ref);
        self.live.error = None;
        self.pending_terminal_claim = None;
        self.seal_model_usage();
        true
    }

    fn fail(&mut self, error: String) -> bool {
        if self.pending_terminal_claim.is_some() {
            return false;
        }
        if !self.transition(ExecutionLiveStatus::Error, Some(error.clone())) {
            return false;
        }
        self.live.error = Some(error.clone());
        self.seal_model_usage();
        true
    }

    fn block(
        &mut self,
        report: &ContextTurnReport,
        write_attempt_paths: &[String],
        terminal_ref: String,
        reason: String,
    ) -> bool {
        if self.live.status == ExecutionLiveStatus::Error
            && self.live.terminal_ref.as_deref() == Some(terminal_ref.as_str())
        {
            self.apply_terminal_projection(report, write_attempt_paths, terminal_ref);
            self.live.error = Some(reason);
            self.touch();
            self.seal_model_usage();
            return true;
        }
        if !self.transition(ExecutionLiveStatus::Error, Some(reason.clone())) {
            return false;
        }
        self.apply_terminal_projection(report, write_attempt_paths, terminal_ref);
        self.live.error = Some(reason.clone());
        self.pending_terminal_claim = None;
        self.seal_model_usage();
        true
    }

    /// Enrich a Session-root projection only after the durable Session
    /// terminal transaction has won and finalized the exact same terminal.
    /// This deliberately cannot transition lifecycle state or clear a pending
    /// delivery claim; those mutations belong exclusively to
    /// `claim_terminal`/`finalize_terminal`/`abort_terminal_claim`.
    fn enrich_finalized_session_terminal(
        &mut self,
        expected_status: ExecutionLiveStatus,
        terminal_ref: String,
        report: &ContextTurnReport,
        write_attempt_paths: &[String],
        error: Option<String>,
    ) -> Result<(), String> {
        if !self.live.status.is_terminal()
            || self.live.status != expected_status
            || self.live.terminal_ref.as_deref() != Some(terminal_ref.as_str())
        {
            return Err(format!(
                "Session terminal enrichment rejected for execution `{}`: expected {expected_status:?} `{terminal_ref}`, found {:?} {:?}",
                self.execution_id, self.live.status, self.live.terminal_ref
            ));
        }
        if self.pending_terminal_claim.is_some() {
            return Err(format!(
                "Session terminal enrichment rejected for execution `{}`: a delivery claim is still pending",
                self.execution_id
            ));
        }
        self.apply_terminal_projection(report, write_attempt_paths, terminal_ref);
        self.live.error = error;
        self.touch();
        self.seal_model_usage();
        Ok(())
    }

    fn cancel(&mut self, detail: String) -> bool {
        if self.pending_terminal_claim.is_some() {
            return false;
        }
        if !self.transition(ExecutionLiveStatus::Cancelled, Some(detail)) {
            return false;
        }
        self.live.error = None;
        self.seal_model_usage();
        true
    }

    fn apply_durable_event(&mut self, event: &DurableRuntimeEvent) {
        let mut changed = false;
        if self.graph_id.is_none() {
            self.graph_id = event
                .activity_binding()
                .map(|binding| binding.root_execution_id);
            changed |= self.graph_id.is_some();
        }
        for reference in &event.refs {
            if reference.kind == "tool_invocation" && self.tool_ids.insert(reference.id.clone()) {
                self.live.metrics.tool_calls = self.live.metrics.tool_calls.saturating_add(1);
                changed = true;
            }
        }
        // `runtime.session.terminal_requested` is an outbox request, not the
        // Session terminal itself. Delivery owns the reversible pending claim
        // and only a committed Session transcript may finalize live status.
        if changed {
            self.live.revision = self.live.revision.saturating_add(1);
            self.live.updated_at_ms = self.live.updated_at_ms.max(event.created_at_ms);
            self.live.last_progress_at_ms = self.live.last_progress_at_ms.max(event.created_at_ms);
            self.refresh_latency();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PendingLiveTerminalClaim {
    terminal_ref: String,
    status: ExecutionLiveStatus,
    session_generation: u64,
}

/// The sole lifecycle reducer for provider-backed session executions.
pub(crate) struct ExecutionLiveStore {
    event_store: Arc<RuntimeEventStore>,
    record_shards: Vec<Mutex<BTreeMap<String, LiveExecutionRecord>>>,
    session_index_shards: Vec<std::sync::RwLock<BTreeMap<String, BTreeSet<String>>>>,
    durable_checkpoints: Mutex<BTreeMap<String, DurableLiveCheckpoint>>,
    checkpoint_gate: Mutex<()>,
    released_terminal_checkpoints: Mutex<BTreeSet<String>>,
    hot_state: Arc<RuntimeHotStatePlane>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalFenceClaim {
    Claimed,
    SamePending,
    SameTerminal,
    ConflictingPending,
    ConflictingTerminal,
    MissingExecution,
}

/// Exact execution-ID partition retained by the live usage owner. Consumers
/// must not infer this split from the flattened presentation metrics.
#[derive(Debug, Clone, Default)]
pub(crate) struct ExecutionModelUsageSnapshot {
    pub(crate) own: Option<crate::RunModelTelemetry>,
    pub(crate) descendants: BTreeMap<String, crate::RunModelTelemetry>,
}

impl ExecutionLiveStore {
    #[cfg(test)]
    pub(crate) fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        Self::with_hot_state(event_store, Arc::new(RuntimeHotStatePlane::default()))
    }

    pub(crate) fn with_hot_state(
        event_store: Arc<RuntimeEventStore>,
        hot_state: Arc<RuntimeHotStatePlane>,
    ) -> Self {
        let (records, durable_checkpoints) = recover_live_records_once(&event_store);
        let shard_count = hot_state.shard_count();
        let record_shards = (0..shard_count)
            .map(|_| Mutex::new(BTreeMap::new()))
            .collect::<Vec<_>>();
        let session_index_shards = (0..shard_count)
            .map(|_| std::sync::RwLock::new(BTreeMap::new()))
            .collect::<Vec<_>>();
        let store = Self {
            event_store,
            record_shards,
            session_index_shards,
            durable_checkpoints: Mutex::new(durable_checkpoints),
            checkpoint_gate: Mutex::new(()),
            released_terminal_checkpoints: Mutex::new(BTreeSet::new()),
            hot_state,
        };
        for (execution_id, record) in records {
            store.index_execution(&record.session_id, &execution_id);
            store.record_shards[store.record_shard(&execution_id)]
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(execution_id, record);
        }
        store.publish_all_residency();
        store
    }

    pub(crate) fn record_queued(&self, session_id: &str, execution_id: String, turn_id: String) {
        let shard_index = self.record_shard(&execution_id);
        if self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&execution_id)
        {
            return;
        }
        let record = self.load_record(&execution_id).unwrap_or_else(|| {
            LiveExecutionRecord::new(session_id.to_string(), execution_id.clone(), turn_id)
        });
        let _ = self.persist(&record);
        self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(execution_id.clone())
            .or_insert(record);
        self.index_execution(session_id, &execution_id);
        self.refresh_hot_session(session_id);
        self.prune_terminal_cache();
    }

    /// Allocate the request-local telemetry generation from the durable live
    /// checkpoint. A process restart therefore continues above the last
    /// admitted lease instead of restarting at one and being rejected by its
    /// own recovered fence.
    pub(crate) fn allocate_model_usage_generation(
        &self,
        execution_id: &str,
    ) -> Result<u64, String> {
        self.allocate_model_usage_generation_with_post_cas(execution_id, |_| {})
    }

    fn allocate_model_usage_generation_with_post_cas(
        &self,
        execution_id: &str,
        post_cas: impl FnOnce(u64),
    ) -> Result<u64, String> {
        const MAX_CAS_ATTEMPTS: usize = 16;
        let mut post_cas = Some(post_cas);
        let _gate = self
            .checkpoint_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self
            .released_terminal_checkpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(execution_id)
        {
            return Err(format!(
                "cannot allocate model telemetry generation for released execution `{execution_id}`"
            ));
        }

        for _ in 0..MAX_CAS_ATTEMPTS {
            let checkpoint = self
                .event_store
                .projection_checkpoint(&live_projection_id(execution_id))
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!(
                        "cannot allocate model telemetry generation before execution `{execution_id}` is durably registered"
                    )
                })?;
            let mut durable_record: LiveExecutionRecord =
                serde_json::from_value(checkpoint.payload.clone()).map_err(|error| {
                    format!(
                        "decode live execution `{execution_id}` for telemetry generation: {error}"
                    )
                })?;
            if durable_record.live.status.is_terminal()
                || durable_record.model_usage_fence_generation.is_some()
            {
                return Err(format!(
                    "cannot allocate model telemetry generation for terminal execution `{execution_id}`"
                ));
            }
            let generation = durable_record
                .model_usage_generation
                .max(
                    durable_record
                        .model_usage_fence_generation
                        .unwrap_or_default(),
                )
                .checked_add(1)
                .ok_or_else(|| {
                    format!("model telemetry generation exhausted for execution `{execution_id}`")
                })?;
            durable_record.model_usage_generation = generation;
            let payload = serde_json::to_value(&durable_record).map_err(|error| {
                format!("encode live execution `{execution_id}` telemetry generation: {error}")
            })?;
            let updated_at_ms = current_time_ms();
            match self.event_store.compare_and_put_projection_checkpoint(
                &checkpoint.projection_id,
                checkpoint.source_cursor,
                checkpoint.revision,
                &payload,
                updated_at_ms,
            ) {
                Ok(updated) => {
                    self.durable_checkpoints
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(
                            execution_id.to_string(),
                            DurableLiveCheckpoint {
                                source_cursor: updated.source_cursor,
                                row_revision: updated.revision,
                                live_revision: durable_record.live.revision,
                                model_usage_generation: durable_record.model_usage_generation,
                                model_usage_fence_generation: durable_record
                                    .model_usage_fence_generation,
                                updated_at_ms: updated.updated_at_ms,
                            },
                        );
                    drop(_gate);
                    if let Some(post_cas) = post_cas.take() {
                        post_cas(generation);
                    }
                    let shard_index = self.record_shard(execution_id);
                    let mut records = self.record_shards[shard_index]
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let resident_record = if let Some(record) = records.get_mut(execution_id) {
                        if record.live.status.is_terminal()
                            || record.model_usage_fence_generation.is_some()
                        {
                            return Err(format!(
                                "execution `{execution_id}` became terminal while allocating model telemetry generation"
                            ));
                        }
                        record.model_usage_generation =
                            record.model_usage_generation.max(generation);
                        record.clone()
                    } else {
                        durable_record
                    };
                    drop(records);
                    self.publish_record_residency(&resident_record);
                    return Ok(generation);
                }
                Err(error) => {
                    tracing::debug!(
                        execution_id,
                        %error,
                        "retrying durable model telemetry generation allocation after checkpoint race"
                    );
                }
            }
        }
        Err(format!(
            "model telemetry generation allocation for execution `{execution_id}` exceeded {MAX_CAS_ATTEMPTS} checkpoint races"
        ))
    }

    pub(crate) fn observe_event(&self, expected_session_id: &str, event: &CowdEvent) {
        self.observe_event_with_generation(expected_session_id, event, 0);
    }

    pub(crate) fn observe_event_with_generation(
        &self,
        expected_session_id: &str,
        event: &CowdEvent,
        model_usage_generation: u64,
    ) {
        let Some(context) = event.execution_context() else {
            return;
        };
        let parent_execution_id = event
            .execution_lineage()
            .map(|lineage| lineage.parent_execution_id.clone())
            .or_else(|| {
                event.activity_binding().and_then(|binding| {
                    (binding.root_execution_id != context.execution_id)
                        .then(|| binding.root_execution_id.clone())
                })
            })
            .filter(|parent_execution_id| parent_execution_id != &context.execution_id);
        if context.session_id != expected_session_id {
            tracing::warn!(
                expected_session_id,
                event_session_id = %context.session_id,
                execution_id = %context.execution_id,
                "ignored execution event delivered through the wrong session relay"
            );
            return;
        }
        let shard_index = self.record_shard(&context.execution_id);
        let missing = !self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&context.execution_id);
        let recovered = missing
            .then(|| self.load_record(&context.execution_id))
            .flatten();
        let mut records = self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = records
            .entry(context.execution_id.clone())
            .or_insert_with(|| {
                recovered.unwrap_or_else(|| {
                    LiveExecutionRecord::new(
                        context.session_id.clone(),
                        context.execution_id.clone(),
                        context.turn_id.clone(),
                    )
                })
            });
        let parent_changed = if let Some(parent_execution_id) = parent_execution_id.as_ref() {
            match record.parent_execution_id.as_ref() {
                None => {
                    record.parent_execution_id = Some(parent_execution_id.clone());
                    true
                }
                Some(existing) if existing != parent_execution_id => {
                    tracing::error!(
                        execution_id = %record.execution_id,
                        existing_parent_execution_id = %existing,
                        observed_parent_execution_id = %parent_execution_id,
                        "ignored conflicting parent identity for Runtime child execution"
                    );
                    false
                }
                Some(_) => false,
            }
        } else {
            false
        };
        let event_changed = match event.domain_event() {
            CowdEvent::ExecutionPhase { status, detail } => {
                record.transition(*status, detail.clone())
            }
            CowdEvent::TextDelta { text } => {
                if let Some(identity) = event.causal_identity() {
                    record.append_preview(identity, text);
                    true
                } else {
                    tracing::warn!(
                        execution_id = %record.execution_id,
                        "ignored text delta without Runtime causal item identity"
                    );
                    false
                }
            }
            CowdEvent::ItemCompleted {
                kind: crate::CausalItemKind::Text,
                ..
            } => {
                if let Some(identity) = event.causal_identity() {
                    record.complete_output_part(identity)
                } else {
                    false
                }
            }
            CowdEvent::ItemCompleted {
                kind: crate::CausalItemKind::ToolCall,
                ..
            } => {
                let Some(tool_call_id) = event
                    .causal_identity()
                    .and_then(|identity| identity.tool_call_id.as_ref())
                else {
                    return;
                };
                if record.tool_ids.insert(tool_call_id.clone()) {
                    record.live.metrics.tool_calls =
                        record.live.metrics.tool_calls.saturating_add(1);
                    record.touch();
                    true
                } else {
                    false
                }
            }
            CowdEvent::ToolStart { id, name, preview } => {
                let mut changed = false;
                if record.tool_ids.insert(id.clone()) {
                    record.live.metrics.tool_calls =
                        record.live.metrics.tool_calls.saturating_add(1);
                    changed = true;
                }
                if let Some(path) = file_touch_path(name, preview) {
                    if record.file_touch_keys.insert(path) {
                        record.live.metrics.files_touched =
                            record.live.metrics.files_touched.saturating_add(1);
                        changed = true;
                    }
                }
                if changed {
                    record.touch();
                }
                changed
            }
            CowdEvent::ToolProgress { .. } | CowdEvent::ToolComplete { .. } => {
                record.touch();
                true
            }
            CowdEvent::ApprovalRequested { request_id, tool } => {
                // The runtime request identity, not the tool label, is the
                // durable projection key. The fallback only preserves
                // compatibility with already-stored pre-v0.9.584 events.
                let key = if request_id.trim().is_empty() {
                    format!("legacy-tool:{tool}")
                } else {
                    request_id.clone()
                };
                if record.approval_keys.insert(key) {
                    record.live.metrics.approvals = record.live.metrics.approvals.saturating_add(1);
                    record.touch();
                    true
                } else {
                    false
                }
            }
            CowdEvent::ApprovalResolved { .. } => {
                record.touch();
                true
            }
            CowdEvent::CapabilityAssessed { .. }
            | CowdEvent::AuthorizationLeaseTransition { .. } => {
                record.touch();
                true
            }
            CowdEvent::ContextWindow(window_tokens) => {
                let mut usage = record.live.context_usage.clone().unwrap_or_default();
                usage.window_tokens = Some(*window_tokens);
                usage.window_source = Some("runtime_event".to_string());
                update_usage_percent(&mut usage);
                record.live.context_usage = Some(usage);
                record.touch();
                true
            }
            CowdEvent::ProviderAttempt {
                model,
                context_window_tokens,
                context_window_source,
                packed_input_tokens,
                ..
            } => {
                let mut usage = record.live.context_usage.clone().unwrap_or_default();
                usage.model = Some(model.clone());
                usage.window_tokens = Some(*context_window_tokens);
                usage.window_source = Some(context_window_source.clone());
                usage.input_tokens = Some(*packed_input_tokens);
                usage.input_source = Some("runtime_request_budget_estimate".to_string());
                update_usage_percent(&mut usage);
                record.live.context_usage = Some(usage);
                record.touch();
                true
            }
            CowdEvent::ContextEnvelope { envelope } => {
                let mut changed = false;
                for item in &envelope.selected {
                    changed |= record.context_item_ids.insert(item.id.clone());
                    if item.source == crate::context_runtime::ContextSourceKind::Memory {
                        changed |= record.memory_item_ids.insert(item.id.clone());
                        for evidence_id in &item.evidence {
                            changed |= record.memory_evidence_ids.insert(evidence_id.clone());
                        }
                    }
                    if matches!(
                        item.source,
                        crate::context_runtime::ContextSourceKind::Fact
                            | crate::context_runtime::ContextSourceKind::Matrix
                    ) {
                        changed |= record.reality_item_ids.insert(item.id.clone());
                    }
                }
                record.live.metrics.context_items =
                    record.context_item_ids.len().try_into().unwrap_or(u64::MAX);
                record.live.metrics.memory_recalls =
                    record.memory_item_ids.len().try_into().unwrap_or(u64::MAX);
                record.live.metrics.memory_evidence = record
                    .memory_evidence_ids
                    .len()
                    .try_into()
                    .unwrap_or(u64::MAX);
                if changed {
                    record.touch();
                }
                changed
            }
            CowdEvent::WriteAttemptsObserved { paths } => {
                let mut changed = false;
                for path in paths {
                    changed |= record.file_touch_keys.insert(path.clone());
                }
                record.live.metrics.files_touched =
                    record.file_touch_keys.len().try_into().unwrap_or(u64::MAX);
                if changed {
                    record.touch();
                }
                changed
            }
            CowdEvent::RunModelTelemetry { telemetry } => {
                if !record.admit_model_usage(model_usage_generation) {
                    return;
                }
                record.own_model_usage = Some(telemetry.clone());
                record.refresh_model_usage_metrics();
                let mut usage = record.live.context_usage.clone().unwrap_or_default();
                usage.model = telemetry.model.clone();
                // RunModelTelemetry is cumulative billed usage for the whole
                // turn. Context occupancy is request-local and is owned by
                // ProviderAttempt/the final context ledger; replacing it here
                // can add multiple model requests together and exceed the
                // actual context window.
                record.live.context_usage = Some(usage);
                record.touch();
                true
            }
            CowdEvent::ExecutionGraphSummary { summary } => {
                let graph_id = summary
                    .graph_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|graph_id| !graph_id.is_empty());
                if graph_id == record.graph_id.as_deref() {
                    false
                } else if let Some(graph_id) = graph_id {
                    record.graph_id = Some(graph_id.to_string());
                    record.touch();
                    true
                } else {
                    false
                }
            }
            CowdEvent::TurnError { error } => record.fail(error.clone()),
            _ => false,
        };
        let changed = parent_changed || event_changed;
        // Streaming preview/progress is recoverable from the live Surface
        // transport and final durable transcript. Persisting a full snapshot
        // for every token held the reducer lock across synchronous storage
        // I/O and amplified one provider stream into unbounded ledger writes.
        // Semantic checkpoints and every terminal transition remain durable.
        let checkpoint = changed
            && !matches!(
                event.domain_event(),
                CowdEvent::TextDelta { .. } | CowdEvent::ToolProgress { .. }
            );
        let lifecycle_boundary = matches!(
            event.domain_event(),
            CowdEvent::ExecutionPhase { .. }
                | CowdEvent::ExecutionGraphSummary { .. }
                | CowdEvent::TurnError { .. }
        );
        let checkpoint_record = checkpoint.then(|| record.clone());
        let hot_record = changed.then(|| record.clone());
        self.emit_long_running_warnings(expected_session_id, record);
        drop(records);
        if let Some(record) = checkpoint_record.as_ref() {
            let _ = self.persist_if_due(record, lifecycle_boundary);
        } else if let Some(record) = hot_record.as_ref() {
            self.publish_record_residency(record);
        }
        if changed {
            self.refresh_hot_session(expected_session_id);
        }
        self.prune_terminal_cache();
        if let Some(parent_execution_id) = parent_execution_id {
            self.observe_descendant_event(
                &parent_execution_id,
                context,
                event,
                model_usage_generation,
            );
        }
    }

    /// Healthy, progressing executions are never killed by a wall-clock
    /// deadline. Every 30 minutes the Runtime surfaces a durable warning so
    /// the user can either keep waiting for the final result or finalize
    /// (collect intermediate results and produce the requested artifact).
    fn emit_long_running_warnings(&self, session_id: &str, record: &mut LiveExecutionRecord) {
        if matches!(
            record.live.status,
            ExecutionLiveStatus::Complete
                | ExecutionLiveStatus::Error
                | ExecutionLiveStatus::Cancelled
        ) {
            return;
        }
        let now = current_time_ms();
        if now <= record.live.started_at_ms {
            return;
        }
        let elapsed_minutes = (now - record.live.started_at_ms) / 60_000;
        let buckets = elapsed_minutes / 30;
        if buckets <= record.warning_buckets {
            return;
        }
        for bucket in (record.warning_buckets + 1)..=buckets {
            let minutes = bucket * 30;
            let _ = self.event_store.append(crate::RuntimeEventInput {
                stream_id: format!("session-live:{session_id}"),
                scope: crate::RuntimeEventScope::Session,
                kind: "runtime.long_running_warning".to_string(),
                status: Some("active".to_string()),
                actor: Some("runtime".to_string()),
                payload: serde_json::json!({
                    "execution_id": record.execution_id,
                    "elapsed_minutes": minutes,
                    "guidance": "执行仍在健康推进。可继续等待最终结果，或调用 finalize 立即回收中间成果并产出当前可交付产物。",
                }),
                refs: Vec::new(),
            });
        }
        record.warning_buckets = buckets;
    }

    fn observe_descendant_event(
        &self,
        parent_execution_id: &str,
        child_context: &crate::CowdExecutionContext,
        event: &CowdEvent,
        model_usage_generation: u64,
    ) {
        let shard_index = self.record_shard(parent_execution_id);
        let mut records = self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = records.get_mut(parent_execution_id) else {
            return;
        };
        let child_prefix = format!("{}:", child_context.execution_id);
        let changed = match event.domain_event() {
            CowdEvent::ItemCompleted {
                kind: crate::CausalItemKind::ToolCall,
                ..
            } => event
                .causal_identity()
                .and_then(|identity| identity.tool_call_id.as_ref())
                .is_some_and(|tool_call_id| {
                    if record
                        .tool_ids
                        .insert(format!("{child_prefix}{tool_call_id}"))
                    {
                        record.live.metrics.tool_calls =
                            record.live.metrics.tool_calls.saturating_add(1);
                        record.touch();
                        true
                    } else {
                        false
                    }
                }),
            CowdEvent::ToolStart { id, name, preview } => {
                let mut changed = false;
                if record.tool_ids.insert(format!("{child_prefix}{id}")) {
                    record.live.metrics.tool_calls =
                        record.live.metrics.tool_calls.saturating_add(1);
                    changed = true;
                }
                if let Some(path) = file_touch_path(name, preview) {
                    if record.file_touch_keys.insert(path) {
                        record.live.metrics.files_touched =
                            record.live.metrics.files_touched.saturating_add(1);
                        changed = true;
                    }
                }
                if changed {
                    record.touch();
                }
                changed
            }
            CowdEvent::ToolProgress { .. } | CowdEvent::ToolComplete { .. } => {
                record.touch();
                true
            }
            CowdEvent::ContextEnvelope { envelope } => {
                let mut changed = false;
                for item in &envelope.selected {
                    let item_id = format!("{child_prefix}{}", item.id);
                    changed |= record.context_item_ids.insert(item_id.clone());
                    if item.source == crate::context_runtime::ContextSourceKind::Memory {
                        changed |= record.memory_item_ids.insert(item_id.clone());
                        for evidence_id in &item.evidence {
                            changed |= record
                                .memory_evidence_ids
                                .insert(format!("{child_prefix}{evidence_id}"));
                        }
                    }
                    if matches!(
                        item.source,
                        crate::context_runtime::ContextSourceKind::Fact
                            | crate::context_runtime::ContextSourceKind::Matrix
                    ) {
                        changed |= record.reality_item_ids.insert(item_id);
                    }
                }
                record.live.metrics.context_items =
                    record.context_item_ids.len().try_into().unwrap_or(u64::MAX);
                record.live.metrics.memory_recalls =
                    record.memory_item_ids.len().try_into().unwrap_or(u64::MAX);
                record.live.metrics.memory_evidence = record
                    .memory_evidence_ids
                    .len()
                    .try_into()
                    .unwrap_or(u64::MAX);
                if changed {
                    record.touch();
                }
                changed
            }
            CowdEvent::WriteAttemptsObserved { paths } => {
                let changed = paths.iter().fold(false, |changed, path| {
                    record.file_touch_keys.insert(path.clone()) || changed
                });
                record.live.metrics.files_touched =
                    record.file_touch_keys.len().try_into().unwrap_or(u64::MAX);
                if changed {
                    record.touch();
                }
                changed
            }
            CowdEvent::RunModelTelemetry { telemetry } => {
                if !record.admit_model_usage(model_usage_generation) {
                    return;
                }
                record
                    .descendant_model_usage
                    .insert(child_context.execution_id.clone(), telemetry.clone());
                record.refresh_model_usage_metrics();
                record.touch();
                true
            }
            _ => false,
        };
        let checkpoint = changed
            && !matches!(
                event.domain_event(),
                CowdEvent::ToolProgress { .. } | CowdEvent::TextDelta { .. }
            );
        let checkpoint_record = checkpoint.then(|| record.clone());
        let hot_record = changed.then(|| record.clone());
        let session_id = record.session_id.clone();
        drop(records);
        if let Some(record) = checkpoint_record.as_ref() {
            let _ = self.persist_if_due(record, false);
        } else if let Some(record) = hot_record.as_ref() {
            self.publish_record_residency(record);
        }
        if changed {
            self.refresh_hot_session(&session_id);
        }
    }

    pub(crate) fn complete(
        &self,
        execution_id: &str,
        report: &ContextTurnReport,
        write_attempt_paths: &[String],
        terminal_ref: String,
    ) {
        self.update_record(execution_id, |record| {
            record.complete(report, write_attempt_paths, terminal_ref)
        });
    }

    pub(crate) fn enrich_finalized_session_terminal(
        &self,
        execution_id: &str,
        expected_status: ExecutionLiveStatus,
        terminal_ref: String,
        report: &ContextTurnReport,
        write_attempt_paths: &[String],
        error: Option<String>,
    ) -> Result<(), String> {
        let shard_index = self.record_shard(execution_id);
        let mut records = self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = records.get_mut(execution_id) else {
            return Err(format!(
                "Session terminal enrichment rejected: execution `{execution_id}` is not registered"
            ));
        };
        let previous = record.clone();
        record.enrich_finalized_session_terminal(
            expected_status,
            terminal_ref,
            report,
            write_attempt_paths,
            error,
        )?;
        let checkpoint = record.clone();
        let session_id = record.session_id.clone();
        if let Err(error) = self.persist_if_due(&checkpoint, true) {
            *record = previous;
            return Err(error);
        }
        drop(records);
        self.refresh_hot_session(&session_id);
        self.prune_terminal_cache();
        Ok(())
    }

    pub(crate) fn claim_terminal(
        &self,
        execution_id: &str,
        terminal_ref: String,
        status: ExecutionLiveStatus,
        session_generation: u64,
    ) -> Result<TerminalFenceClaim, String> {
        // Terminal claims race live checkpoint writers. Refresh the durable row
        // revision before the CAS and retry on a stale revision instead of
        // surfacing a projection invariant failure to the caller.
        let mut last_stale = None;
        for attempt in 0..3 {
            let _ = self.reload_record_from_durable(execution_id);
            match self.claim_terminal_once(
                execution_id,
                terminal_ref.clone(),
                status,
                session_generation,
            ) {
                Err(error) if error.contains("revision mismatch") && attempt < 2 => {
                    last_stale = Some(error);
                }
                other => return other,
            }
        }
        Err(last_stale.unwrap_or_else(|| "terminal claim retry exhausted".to_string()))
    }

    fn claim_terminal_once(
        &self,
        execution_id: &str,
        terminal_ref: String,
        status: ExecutionLiveStatus,
        session_generation: u64,
    ) -> Result<TerminalFenceClaim, String> {
        let shard_index = self.record_shard(execution_id);
        let mut records = self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = records.get_mut(execution_id) else {
            return Ok(TerminalFenceClaim::MissingExecution);
        };
        if record.live.status.is_terminal() {
            if record.live.status == status
                && record.live.terminal_ref.as_deref() == Some(terminal_ref.as_str())
            {
                return Ok(TerminalFenceClaim::SameTerminal);
            }
            return Ok(TerminalFenceClaim::ConflictingTerminal);
        }
        if let Some(pending) = record.pending_terminal_claim.as_ref() {
            if pending.terminal_ref == terminal_ref
                && pending.status == status
                && pending.session_generation == session_generation
            {
                return Ok(TerminalFenceClaim::SamePending);
            }
            return Ok(TerminalFenceClaim::ConflictingPending);
        }
        let previous = record.clone();
        record.pending_terminal_claim = Some(PendingLiveTerminalClaim {
            terminal_ref,
            status,
            session_generation,
        });
        record.touch();
        let checkpoint = record.clone();
        let session_id = record.session_id.clone();
        if let Err(error) = self.persist_if_due(&checkpoint, true) {
            *record = previous;
            return Err(error);
        }
        drop(records);
        self.refresh_hot_session(&session_id);
        self.prune_terminal_cache();
        Ok(TerminalFenceClaim::Claimed)
    }

    pub(crate) fn finalize_terminal(
        &self,
        execution_id: &str,
        terminal_ref: &str,
        status: ExecutionLiveStatus,
        session_generation: u64,
    ) -> Result<TerminalFenceClaim, String> {
        let mut last_stale = None;
        for attempt in 0..3 {
            let _ = self.reload_record_from_durable(execution_id);
            match self.finalize_terminal_once(
                execution_id,
                terminal_ref,
                status,
                session_generation,
            ) {
                Err(error) if error.contains("revision mismatch") && attempt < 2 => {
                    last_stale = Some(error);
                }
                other => return other,
            }
        }
        Err(last_stale.unwrap_or_else(|| "terminal finalize retry exhausted".to_string()))
    }

    fn finalize_terminal_once(
        &self,
        execution_id: &str,
        terminal_ref: &str,
        status: ExecutionLiveStatus,
        session_generation: u64,
    ) -> Result<TerminalFenceClaim, String> {
        let shard_index = self.record_shard(execution_id);
        let mut records = self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = records.get_mut(execution_id) else {
            return Ok(TerminalFenceClaim::MissingExecution);
        };
        if record.live.status.is_terminal() {
            return Ok(
                if record.live.status == status
                    && record.live.terminal_ref.as_deref() == Some(terminal_ref)
                {
                    TerminalFenceClaim::SameTerminal
                } else {
                    TerminalFenceClaim::ConflictingTerminal
                },
            );
        }
        let Some(pending) = record.pending_terminal_claim.as_ref() else {
            return Ok(TerminalFenceClaim::ConflictingPending);
        };
        if pending.terminal_ref != terminal_ref
            || pending.status != status
            || pending.session_generation != session_generation
        {
            return Ok(TerminalFenceClaim::ConflictingPending);
        }
        let previous = record.clone();
        if !record.transition(
            status,
            Some("durable Session terminal committed".to_string()),
        ) {
            return Ok(TerminalFenceClaim::ConflictingTerminal);
        }
        record.live.terminal_ref = Some(terminal_ref.to_string());
        record.live.error = (status == ExecutionLiveStatus::Error)
            .then(|| "Runtime turn completed without full satisfaction".to_string());
        record.pending_terminal_claim = None;
        record.seal_model_usage();
        let checkpoint = record.clone();
        let session_id = record.session_id.clone();
        if let Err(error) = self.persist_if_due(&checkpoint, true) {
            *record = previous;
            return Err(error);
        }
        drop(records);
        self.refresh_hot_session(&session_id);
        self.prune_terminal_cache();
        Ok(TerminalFenceClaim::Claimed)
    }

    pub(crate) fn abort_terminal_claim(
        &self,
        execution_id: &str,
        terminal_ref: &str,
        session_generation: u64,
    ) -> Result<bool, String> {
        let _ = self.execution_live(execution_id);
        let shard_index = self.record_shard(execution_id);
        let mut records = self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = records.get_mut(execution_id) else {
            return Ok(false);
        };
        let matches = record
            .pending_terminal_claim
            .as_ref()
            .is_some_and(|pending| {
                pending.terminal_ref == terminal_ref
                    && pending.session_generation == session_generation
            });
        if !matches {
            return Ok(false);
        }
        let previous = record.clone();
        record.pending_terminal_claim = None;
        record.touch();
        let checkpoint = record.clone();
        let session_id = record.session_id.clone();
        if let Err(error) = self.persist_if_due(&checkpoint, true) {
            *record = previous;
            return Err(error);
        }
        drop(records);
        self.refresh_hot_session(&session_id);
        Ok(true)
    }

    pub(crate) fn release_terminal_checkpoint(&self, execution_id: &str) {
        let _gate = self
            .checkpoint_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.released_terminal_checkpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(execution_id.to_string());
        match self
            .event_store
            .delete_projection_checkpoint(&live_projection_id(execution_id))
        {
            Ok(_) => {
                self.durable_checkpoints
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(execution_id);
            }
            Err(error) => tracing::error!(
                execution_id,
                %error,
                "failed to release delivered terminal live checkpoint"
            ),
        }
    }

    pub(crate) fn fail(&self, execution_id: &str, error: String) {
        self.update_record(execution_id, |record| record.fail(error));
    }

    pub(crate) fn block(
        &self,
        execution_id: &str,
        report: &ContextTurnReport,
        write_attempt_paths: &[String],
        terminal_ref: String,
        reason: String,
    ) {
        self.update_record(execution_id, |record| {
            record.block(report, write_attempt_paths, terminal_ref, reason)
        });
    }

    pub(crate) fn cancel(&self, execution_id: &str, detail: String) -> Result<bool, String> {
        let shard_index = self.record_shard(execution_id);
        let mut records = self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = records.get_mut(execution_id) else {
            return Ok(false);
        };
        let previous = record.clone();
        if !record.cancel(detail) {
            return Ok(false);
        }
        let checkpoint = record.clone();
        if let Err(error) = self.persist_if_due(&checkpoint, true) {
            *record = previous;
            return Err(error);
        }
        let session_id = record.session_id.clone();
        drop(records);
        self.refresh_hot_session(&session_id);
        self.prune_terminal_cache();
        Ok(true)
    }

    pub(crate) fn execution_live(&self, execution_id: &str) -> Option<ExecutionLiveState> {
        if let Some(live) = self.execution_live_hot(execution_id) {
            return Some(live);
        }
        let shard_index = self.record_shard(execution_id);
        let record = self.load_record(execution_id)?;
        let live = record.live.clone();
        self.index_execution(&record.session_id, execution_id);
        self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(execution_id.to_string(), record);
        Some(live)
    }

    pub(crate) fn model_usage_snapshot(
        &self,
        execution_id: &str,
    ) -> Option<ExecutionModelUsageSnapshot> {
        let _ = self.execution_live(execution_id)?;
        let shard_index = self.record_shard(execution_id);
        self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(execution_id)
            .map(|record| ExecutionModelUsageSnapshot {
                own: record.own_model_usage.clone(),
                descendants: record.descendant_model_usage.clone(),
            })
    }

    fn execution_live_hot(&self, execution_id: &str) -> Option<ExecutionLiveState> {
        let shard_index = self.record_shard(execution_id);
        self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(execution_id)
            .cloned()
            .map(|record| record.live)
    }

    pub(crate) fn session_execution_index(
        &self,
        session_id: &str,
    ) -> SessionExecutionIndexProjection {
        let mut records = self.records_for_session(session_id);
        records.retain(|record| record.parent_execution_id.is_none());
        // Millisecond timestamps can tie when an execution is queued and
        // completed in the same scheduler tick. Revision is the authoritative
        // in-execution ordering signal, so use it before the stable identity
        // tie-breaker instead of exposing an older running record as latest.
        records.sort_by(|left, right| {
            left.live
                .updated_at_ms
                .cmp(&right.live.updated_at_ms)
                .then_with(|| left.live.revision.cmp(&right.live.revision))
                .then_with(|| left.execution_id.cmp(&right.execution_id))
        });
        let latest = records.last();
        let projection = SessionExecutionIndexProjection {
            session_id: session_id.to_string(),
            executions: records
                .iter()
                .map(|record| SessionExecutionEntryProjection {
                    execution_id: record.execution_id.clone(),
                    graph_id: record.graph_id.clone(),
                    turn_id: record.live.turn_id.clone(),
                    status: record.live.status,
                    live_revision: Some(record.live.revision),
                    started_at_ms: Some(record.live.started_at_ms),
                    updated_at_ms: record.live.updated_at_ms,
                    terminal_ref: record.live.terminal_ref.clone(),
                })
                .collect(),
            active_execution_ids: records
                .iter()
                .filter(|record| !record.live.status.is_terminal())
                .map(|record| record.execution_id.clone())
                .collect(),
            latest_execution_id: latest.map(|record| record.execution_id.clone()),
            latest_graph_id: latest.and_then(|record| record.graph_id.clone()),
            latest_status: latest.map(|record| record.live.status),
            latest_live_revision: latest.map(|record| record.live.revision),
            last_progress_at_ms: latest.map(|record| record.live.last_progress_at_ms),
            terminal_ref: latest.and_then(|record| record.live.terminal_ref.clone()),
        };
        self.refresh_hot_session(session_id);
        projection
    }

    pub(crate) fn running_session_execution_indices(&self) -> Vec<SessionExecutionIndexProjection> {
        let mut session_ids = self
            .all_records()
            .into_iter()
            .filter(|record| !record.live.status.is_terminal())
            .map(|record| record.session_id)
            .collect::<BTreeSet<_>>();
        let mut indices = Vec::new();
        for session_id in std::mem::take(&mut session_ids) {
            let index = self.session_execution_index(&session_id);
            if !index.active_execution_ids.is_empty() {
                indices.push(index);
            }
        }
        indices
    }

    fn update_record(
        &self,
        execution_id: &str,
        update: impl FnOnce(&mut LiveExecutionRecord) -> bool,
    ) -> bool {
        let shard_index = self.record_shard(execution_id);
        let mut records = self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = records.get_mut(execution_id) else {
            tracing::warn!(
                execution_id,
                "Runtime live update ignored because execution was never registered"
            );
            return false;
        };
        let changed = update(record);
        let checkpoint_record = changed.then(|| record.clone());
        drop(records);
        if let Some(record) = checkpoint_record.as_ref() {
            if let Err(error) = self.persist_if_due(record, true) {
                tracing::error!(execution_id, %error, "failed to persist Runtime live update");
            }
            self.refresh_hot_session(&record.session_id);
        }
        self.prune_terminal_cache();
        changed
    }

    fn records_for_session(&self, session_id: &str) -> Vec<LiveExecutionRecord> {
        let session_shard = self.session_shard(session_id);
        let execution_ids = self.session_index_shards[session_shard]
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
            .unwrap_or_default();
        execution_ids
            .into_iter()
            .filter_map(|execution_id| {
                self.record_shards[self.record_shard(&execution_id)]
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&execution_id)
                    .cloned()
            })
            .collect()
    }

    fn all_records(&self) -> Vec<LiveExecutionRecord> {
        self.record_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Force-refresh the in-memory record and durable row revision from the
    /// authoritative checkpoint. Used before terminal claims so the CAS never
    /// fails on a cached revision that another writer has already advanced.
    fn reload_record_from_durable(&self, execution_id: &str) -> bool {
        let Ok(Some(checkpoint)) = self
            .event_store
            .projection_checkpoint(&live_projection_id(execution_id))
        else {
            return false;
        };
        let Ok(record) = serde_json::from_value::<LiveExecutionRecord>(checkpoint.payload.clone())
        else {
            return false;
        };
        self.durable_checkpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                execution_id.to_string(),
                DurableLiveCheckpoint {
                    source_cursor: checkpoint.source_cursor,
                    row_revision: checkpoint.revision,
                    live_revision: record.live.revision,
                    model_usage_generation: record.model_usage_generation,
                    model_usage_fence_generation: record.model_usage_fence_generation,
                    updated_at_ms: checkpoint.updated_at_ms,
                },
            );
        let shard_index = self.record_shard(execution_id);
        self.record_shards[shard_index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(execution_id.to_string(), record);
        true
    }

    /// Refresh only the cached durable row revision (and live revision when the
    /// payload decodes). Safe to call while a caller holds the record-shard
    /// lock because it never touches the shard.
    fn refresh_durable_cache_revision(&self, execution_id: &str) {
        let Ok(Some(checkpoint)) = self
            .event_store
            .projection_checkpoint(&live_projection_id(execution_id))
        else {
            return;
        };
        let revision = checkpoint.revision;
        let source_cursor = checkpoint.source_cursor;
        let updated_at_ms = checkpoint.updated_at_ms;
        let mut checkpoints = self
            .durable_checkpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Ok(record) = serde_json::from_value::<LiveExecutionRecord>(checkpoint.payload.clone())
        else {
            // The payload is always a live record today; if it is ever not, at
            // least advance the row revision so the retry does not spin stale.
            checkpoints
                .entry(execution_id.to_string())
                .and_modify(|durable| {
                    durable.row_revision = durable.row_revision.max(revision);
                    durable.source_cursor = durable.source_cursor.max(source_cursor);
                    durable.updated_at_ms = durable.updated_at_ms.max(updated_at_ms);
                });
            return;
        };
        checkpoints.insert(
            execution_id.to_string(),
            DurableLiveCheckpoint {
                source_cursor: checkpoint.source_cursor,
                row_revision: checkpoint.revision,
                live_revision: record.live.revision,
                model_usage_generation: record.model_usage_generation,
                model_usage_fence_generation: record.model_usage_fence_generation,
                updated_at_ms: checkpoint.updated_at_ms,
            },
        );
    }

    fn load_record(&self, execution_id: &str) -> Option<LiveExecutionRecord> {
        let checkpoint = self
            .event_store
            .projection_checkpoint(&live_projection_id(execution_id))
            .ok()
            .flatten()?;
        let mut record: LiveExecutionRecord =
            serde_json::from_value(checkpoint.payload.clone()).ok()?;
        let source_cursor = replay_durable_events(
            self.event_store.as_ref(),
            &mut record,
            checkpoint.source_cursor,
        );
        let checkpoint = persist_replayed_checkpoint(
            self.event_store.as_ref(),
            checkpoint,
            source_cursor,
            &record,
        )
        .ok()?;
        self.durable_checkpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                execution_id.to_string(),
                DurableLiveCheckpoint {
                    source_cursor: checkpoint.source_cursor,
                    row_revision: checkpoint.revision,
                    live_revision: record.live.revision,
                    model_usage_generation: record.model_usage_generation,
                    model_usage_fence_generation: record.model_usage_fence_generation,
                    updated_at_ms: checkpoint.updated_at_ms,
                },
            );
        Some(record)
    }

    fn persist(&self, record: &LiveExecutionRecord) -> Result<(), String> {
        let _gate = self
            .checkpoint_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self
            .released_terminal_checkpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&record.execution_id)
        {
            return Ok(());
        }
        // A concurrent live writer can advance the projection row between our
        // cached revision and the CAS. Refresh the cached row revision and retry
        // instead of dropping the checkpoint.
        let mut attempt = 0;
        loop {
            let durable = self
                .durable_checkpoints
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&record.execution_id)
                .copied();
            if durable.is_some_and(|checkpoint| checkpoint.live_revision > record.live.revision) {
                return Ok(());
            }
            // Generation allocation is a durable lease operation and deliberately
            // does not bump the user-visible live revision. A record clone made
            // before that CAS may therefore arrive here with an equal or newer
            // live revision. Preserve the monotonic lease/fence dimensions before
            // serializing so such a clone cannot roll a restart back to an old
            // generation. `Some` is an irreversible terminal fence.
            let mut candidate = record.clone();
            if let Some(checkpoint) = durable {
                candidate.model_usage_generation = candidate
                    .model_usage_generation
                    .max(checkpoint.model_usage_generation);
                candidate.model_usage_fence_generation = match (
                    candidate.model_usage_fence_generation,
                    checkpoint.model_usage_fence_generation,
                ) {
                    (Some(left), Some(right)) => Some(left.max(right)),
                    (Some(value), None) | (None, Some(value)) => Some(value),
                    (None, None) => None,
                };
            }
            let payload = match serde_json::to_value(&candidate) {
                Ok(payload) => payload,
                Err(error) => {
                    tracing::error!(
                        execution_id = %record.execution_id,
                        %error,
                        "failed to serialize Runtime live execution checkpoint"
                    );
                    self.publish_record_residency(&candidate);
                    return Err(error.to_string());
                }
            };
            let updated_at_ms = current_time_ms();
            let source_cursor = durable.map_or_else(
                || self.event_store.current_commit_cursor(),
                |checkpoint| {
                    self.event_store
                        .current_commit_cursor()
                        .max(checkpoint.source_cursor)
                },
            );
            let expected_revision = durable.map_or(0, |checkpoint| checkpoint.row_revision);
            match self.event_store.compare_and_put_projection_checkpoint(
                &live_projection_id(&record.execution_id),
                source_cursor,
                expected_revision,
                &payload,
                updated_at_ms,
            ) {
                Ok(checkpoint) => {
                    self.durable_checkpoints
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(
                            record.execution_id.clone(),
                            DurableLiveCheckpoint {
                                source_cursor: checkpoint.source_cursor,
                                row_revision: checkpoint.revision,
                                live_revision: candidate.live.revision,
                                model_usage_generation: candidate.model_usage_generation,
                                model_usage_fence_generation: candidate
                                    .model_usage_fence_generation,
                                updated_at_ms: checkpoint.updated_at_ms,
                            },
                        );
                    self.publish_record_residency(&candidate);
                    return Ok(());
                }
                Err(crate::RuntimeEventStoreError::StaleRevision { .. }) if attempt < 2 => {
                    // Refresh only the cached row revision. Callers may hold the
                    // record-shard lock while persisting, so re-locking the shard
                    // here would self-deadlock.
                    self.refresh_durable_cache_revision(&record.execution_id);
                    attempt += 1;
                }
                Err(error) => {
                    tracing::error!(
                        execution_id = %record.execution_id,
                        error = %error,
                            "failed to persist Runtime live execution checkpoint"
                    );
                    return Err(error.to_string());
                }
            }
        }
    }

    fn persist_if_due(&self, record: &LiveExecutionRecord, boundary: bool) -> Result<(), String> {
        let policy = self.hot_state.live_checkpoint_config();
        let now = current_time_ms();
        let due = self
            .durable_checkpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&record.execution_id)
            .copied()
            .map_or(true, |checkpoint| {
                record
                    .live
                    .revision
                    .saturating_sub(checkpoint.live_revision)
                    >= policy.max_revision_gap
                    || now.saturating_sub(checkpoint.updated_at_ms) >= policy.min_interval_ms
            });
        if boundary
            || due
            || record.live.status.is_terminal()
            || record.live.status == ExecutionLiveStatus::WaitingApproval
        {
            self.persist(record)
        } else {
            self.publish_record_residency(record);
            Ok(())
        }
    }

    fn publish_all_residency(&self) {
        for record in self.all_records() {
            self.publish_record_residency(&record);
        }
    }

    fn publish_record_residency(&self, record: &LiveExecutionRecord) {
        let bytes = serde_json::to_vec(record)
            .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            .unwrap_or_default();
        self.hot_state.residency().upsert(
            format!("execution-live:{}", record.execution_id),
            HotResidentClass::DerivedProjection,
            record.execution_id.clone(),
            bytes,
            Some(record.live.revision),
        );
    }

    fn prune_terminal_cache(&self) {
        if !self.hot_state.residency().pressure_high() {
            return;
        }
        for candidate in self
            .hot_state
            .residency()
            .eviction_candidates(HotResidentClass::DerivedProjection)
        {
            if self.hot_state.residency().resident_bytes()
                <= self.hot_state.residency().target_low_watermark()
            {
                break;
            }
            let execution_id = candidate.owner_id;
            let shard_index = self.record_shard(&execution_id);
            let mut records = self.record_shards[shard_index]
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let terminal_session = records
                .get(&execution_id)
                .filter(|record| record.live.status.is_terminal())
                .map(|record| record.session_id.clone());
            if let Some(session_id) = terminal_session {
                records.remove(&execution_id);
                drop(records);
                self.remove_indexed_execution(&session_id, &execution_id);
                self.hot_state
                    .residency()
                    .remove(&format!("execution-live:{execution_id}"));
            }
        }
    }

    fn index_execution(&self, session_id: &str, execution_id: &str) {
        self.session_index_shards[self.session_shard(session_id)]
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(session_id.to_string())
            .or_default()
            .insert(execution_id.to_string());
    }

    fn remove_indexed_execution(&self, session_id: &str, execution_id: &str) {
        let mut shard = self.session_index_shards[self.session_shard(session_id)]
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let remove_session = shard.get_mut(session_id).is_some_and(|execution_ids| {
            execution_ids.remove(execution_id);
            execution_ids.is_empty()
        });
        if remove_session {
            shard.remove(session_id);
            self.clear_hot_session_execution(session_id);
        }
    }

    fn refresh_hot_session(&self, session_id: &str) {
        let records = self.records_for_session(session_id);
        if records.is_empty() {
            self.clear_hot_session_execution(session_id);
            return;
        }
        let mut graph_refs = records
            .iter()
            .filter_map(|record| record.graph_id.clone())
            .collect::<Vec<_>>();
        graph_refs.sort();
        graph_refs.dedup();
        let mut context_refs = records
            .iter()
            .flat_map(|record| record.context_item_ids.iter().cloned())
            .collect::<Vec<_>>();
        context_refs.sort();
        context_refs.dedup();
        let mut memory_refs = records
            .iter()
            .flat_map(|record| record.memory_item_ids.iter().cloned())
            .collect::<Vec<_>>();
        memory_refs.sort();
        memory_refs.dedup();
        let mut reality_refs = records
            .iter()
            .flat_map(|record| record.reality_item_ids.iter().cloned())
            .collect::<Vec<_>>();
        reality_refs.sort();
        reality_refs.dedup();
        let revision = records
            .iter()
            .map(|record| record.live.revision)
            .max()
            .unwrap_or_default();
        self.hot_state.sessions().update(session_id, |snapshot| {
            snapshot.runtime_cursor = snapshot.runtime_cursor.max(revision);
            snapshot.current_execution_ids = records
                .iter()
                .filter(|record| !record.live.status.is_terminal())
                .map(|record| record.execution_id.clone())
                .collect();
            snapshot.execution_graph_refs = graph_refs;
            snapshot.context_refs = context_refs;
            snapshot.memory_refs = memory_refs;
            snapshot.reality_refs = reality_refs;
            snapshot.input_tokens = records
                .iter()
                .map(|record| record.live.metrics.input_tokens)
                .sum();
            snapshot.output_tokens = records
                .iter()
                .map(|record| record.live.metrics.output_tokens)
                .sum();
        });
    }

    fn clear_hot_session_execution(&self, session_id: &str) {
        self.hot_state.sessions().update(session_id, |snapshot| {
            snapshot.current_execution_ids.clear();
            snapshot.execution_graph_refs.clear();
            snapshot.context_refs.clear();
            snapshot.memory_refs.clear();
            snapshot.reality_refs.clear();
            snapshot.input_tokens = 0;
            snapshot.output_tokens = 0;
        });
    }

    fn record_shard(&self, execution_id: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        execution_id.hash(&mut hasher);
        (hasher.finish() as usize) & (self.record_shards.len() - 1)
    }

    fn session_shard(&self, session_id: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        session_id.hash(&mut hasher);
        (hasher.finish() as usize) & (self.session_index_shards.len() - 1)
    }
}

fn file_touch_path(tool_name: &str, preview: &str) -> Option<String> {
    if !matches!(
        tool_name,
        "write_file" | "edit_file" | "apply_patch" | "apply_patch_transaction"
    ) {
        return None;
    }
    let input = serde_json::from_str::<serde_json::Value>(preview).ok()?;
    ["path", "file_path", "target_path"]
        .into_iter()
        .find_map(|key| {
            input
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(ToOwned::to_owned)
        })
}

fn recover_live_records_once(
    event_store: &RuntimeEventStore,
) -> (
    BTreeMap<String, LiveExecutionRecord>,
    BTreeMap<String, DurableLiveCheckpoint>,
) {
    let mut records = BTreeMap::new();
    let mut durable_checkpoints = BTreeMap::new();
    let Ok(checkpoints) = event_store.projection_checkpoints_with_prefix(LIVE_PROJECTION_PREFIX)
    else {
        return (records, durable_checkpoints);
    };
    for checkpoint in checkpoints {
        let Ok(mut record) =
            serde_json::from_value::<LiveExecutionRecord>(checkpoint.payload.clone())
        else {
            continue;
        };
        let source_cursor =
            replay_durable_events(event_store, &mut record, checkpoint.source_cursor);
        let Ok(checkpoint) =
            persist_replayed_checkpoint(event_store, checkpoint, source_cursor, &record)
        else {
            continue;
        };
        durable_checkpoints.insert(
            record.execution_id.clone(),
            DurableLiveCheckpoint {
                source_cursor: checkpoint.source_cursor,
                row_revision: checkpoint.revision,
                live_revision: record.live.revision,
                model_usage_generation: record.model_usage_generation,
                model_usage_fence_generation: record.model_usage_fence_generation,
                updated_at_ms: checkpoint.updated_at_ms,
            },
        );
        records.insert(record.execution_id.clone(), record);
    }
    (records, durable_checkpoints)
}

fn replay_durable_events(
    event_store: &RuntimeEventStore,
    record: &mut LiveExecutionRecord,
    source_cursor: u64,
) -> u64 {
    const PAGE_SIZE: usize = 512;
    let mut position = Some((source_cursor, u32::MAX));
    let mut applied_cursor = source_cursor;
    loop {
        let page = match event_store.events_for_root_execution(
            &record.execution_id,
            position,
            PAGE_SIZE,
        ) {
            Ok(page) => page,
            Err(error) => {
                tracing::error!(
                    execution_id = %record.execution_id,
                    %error,
                    "failed to replay canonical Runtime events into live checkpoint"
                );
                break;
            }
        };
        if page.is_empty() {
            break;
        }
        for event in &page {
            record.apply_durable_event(event);
            applied_cursor = applied_cursor.max(event.commit_cursor);
            position = Some((event.commit_cursor, event.transaction_index));
        }
        if page.len() < PAGE_SIZE || record.live.status.is_terminal() {
            break;
        }
    }
    applied_cursor
}

fn persist_replayed_checkpoint(
    event_store: &RuntimeEventStore,
    checkpoint: RuntimeProjectionCheckpoint,
    source_cursor: u64,
    record: &LiveExecutionRecord,
) -> Result<RuntimeProjectionCheckpoint, String> {
    let payload = serde_json::to_value(record).map_err(|error| error.to_string())?;
    if source_cursor == checkpoint.source_cursor && payload == checkpoint.payload {
        return Ok(checkpoint);
    }
    event_store
        .compare_and_put_projection_checkpoint(
            &checkpoint.projection_id,
            source_cursor,
            checkpoint.revision,
            &payload,
            current_time_ms(),
        )
        .map_err(|error| error.to_string())
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn allows_transition(from: ExecutionLiveStatus, to: ExecutionLiveStatus) -> bool {
    use ExecutionLiveStatus::{
        CallingModel, CallingTool, Cancelled, Complete, Error, Finalizing, PreparingContext,
        Queued, Thinking, WaitingApproval,
    };
    if from == to {
        return true;
    }
    matches!(
        (from, to),
        (
            Queued,
            PreparingContext | CallingModel | Complete | Error | Cancelled
        ) | (
            PreparingContext,
            CallingModel
                | Thinking
                | CallingTool
                | WaitingApproval
                | Finalizing
                | Error
                | Cancelled
        ) | (
            CallingModel,
            Thinking | CallingTool | WaitingApproval | Finalizing | Complete | Error | Cancelled
        ) | (
            Thinking,
            CallingModel
                | CallingTool
                | WaitingApproval
                | Finalizing
                | Complete
                | Error
                | Cancelled
        ) | (
            CallingTool,
            CallingModel | Thinking | WaitingApproval | Finalizing | Complete | Error | Cancelled
        ) | (
            WaitingApproval,
            CallingModel | CallingTool | Thinking | Finalizing | Error | Cancelled // Finalizing is a presentation phase, not a durable terminal commit.
                                                                                   // Terminal synthesis may still be superseded by a quality-gate retry
                                                                                   // or a graph replan.  Only Complete/Error/Cancelled are absorbing.
        ) | (
            Finalizing,
            PreparingContext
                | CallingModel
                | Thinking
                | CallingTool
                | WaitingApproval
                | Complete
                | Error
                | Cancelled
        )
    )
}

fn is_terminal_live_status(status: ExecutionLiveStatus) -> bool {
    matches!(
        status,
        ExecutionLiveStatus::Complete | ExecutionLiveStatus::Error | ExecutionLiveStatus::Cancelled
    )
}

fn context_usage_from_report(report: &ContextTurnReport) -> Option<ContextUsageProjection> {
    let ledger = report.ledger.as_ref()?;
    let input_tokens = ledger
        .calibrated_input_tokens
        .unwrap_or(ledger.consumed_tokens);
    let input_source = if ledger.calibrated_input_tokens.is_some() {
        "provider_actual"
    } else {
        "ledger_estimate"
    };
    let usage_percent_bp = (ledger.max_tokens > 0).then(|| {
        ((u128::from(input_tokens) * 10_000) / u128::from(ledger.max_tokens))
            .min(u128::from(u16::MAX)) as u16
    });
    Some(ContextUsageProjection {
        model: None,
        window_tokens: Some(ledger.max_tokens),
        window_source: Some("runtime_ledger".to_string()),
        input_tokens: Some(input_tokens),
        input_source: Some(input_source.to_string()),
        remaining_tokens: Some(ledger.max_tokens.saturating_sub(input_tokens)),
        usage_percent_bp,
        request_sequence: Some(ledger.request_sequence),
        components: ledger.components.clone(),
    })
}

fn update_usage_percent(usage: &mut ContextUsageProjection) {
    if let (Some(input), Some(window)) = (usage.input_tokens, usage.window_tokens) {
        usage.remaining_tokens = Some(window.saturating_sub(input));
        usage.usage_percent_bp = (window > 0).then(|| {
            ((u128::from(input) * 10_000) / u128::from(window)).min(u128::from(u16::MAX)) as u16
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CowdExecutionContext, RuntimeEventInput, RuntimeEventScope, RuntimeEventStore};

    #[test]
    fn completed_tool_plan_is_visible_before_execution_and_counted_once() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(event_store);
        let context = CowdExecutionContext {
            execution_id: "execution-tool-plan".to_string(),
            session_id: "session-tool-plan".to_string(),
            turn_id: "turn-tool-plan".to_string(),
        };
        store.record_queued(
            &context.session_id,
            context.execution_id.clone(),
            context.turn_id.clone(),
        );
        let identity = crate::CausalItemIdentity {
            model_step_id: "step-tool-plan".to_string(),
            item_id: "call-date".to_string(),
            segment_id: "call-date:tool-call:0".to_string(),
            causal_sequence: 1,
            delta_sequence: 1,
            tool_call_id: Some("call-date".to_string()),
            causal_parent_ids: Vec::new(),
        };
        for event in [
            CowdEvent::Causal {
                identity,
                event: Box::new(CowdEvent::ItemCompleted {
                    kind: crate::CausalItemKind::ToolCall,
                    tool_name: Some("bash".to_string()),
                    tool_input: Some(r#"{"command":"date +%Y"}"#.to_string()),
                }),
            },
            CowdEvent::ToolStart {
                id: "call-date".to_string(),
                name: "bash".to_string(),
                preview: "date +%Y".to_string(),
            },
        ] {
            store.observe_event(
                &context.session_id,
                &CowdEvent::ExecutionScoped {
                    context: context.clone(),
                    activity_binding: None,
                    event: Box::new(event),
                },
            );
        }

        assert_eq!(
            store
                .execution_live(&context.execution_id)
                .unwrap()
                .metrics
                .tool_calls,
            1,
        );
    }

    #[test]
    fn descendant_tool_activity_aggregates_into_root_without_changing_root_phase() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        store.record_queued(
            "session-team",
            "root-execution".to_string(),
            "turn-team".to_string(),
        );
        let event = CowdEvent::RelatedExecution {
            lineage: crate::CowdExecutionLineage {
                parent_execution_id: "root-execution".to_string(),
                graph_id: "team-graph".to_string(),
                node_id: "researcher:1".to_string(),
                team_id: Some("team-run".to_string()),
                agent_id: Some("researcher".to_string()),
            },
            event: Box::new(CowdEvent::ExecutionScoped {
                context: CowdExecutionContext {
                    execution_id: "agent-run".to_string(),
                    session_id: "session-team".to_string(),
                    turn_id: "turn-team".to_string(),
                },
                activity_binding: None,
                event: Box::new(CowdEvent::ToolStart {
                    id: "search-call".to_string(),
                    name: "web_search".to_string(),
                    preview: r#"{"query":"technical standard"}"#.to_string(),
                }),
            }),
        };

        store.observe_event("session-team", &event);

        let root = store.execution_live("root-execution").unwrap();
        assert_eq!(root.status, ExecutionLiveStatus::Queued);
        assert_eq!(root.metrics.tool_calls, 1);
        assert_eq!(
            store
                .execution_live("agent-run")
                .unwrap()
                .metrics
                .tool_calls,
            1
        );
        let store = ExecutionLiveStore::new(event_store);
        let records = store.records_for_session("session-team");
        assert_eq!(
            records
                .iter()
                .find(|record| record.execution_id == "agent-run")
                .and_then(|record| record.parent_execution_id.as_deref()),
            Some("root-execution")
        );
        let index = store.session_execution_index("session-team");
        assert_eq!(index.latest_execution_id.as_deref(), Some("root-execution"));
        assert_eq!(index.executions.len(), 1);
        assert_eq!(index.active_execution_ids, vec!["root-execution"]);

        let report = ContextTurnReport::new(
            "turn-team",
            harness_contract::context::ContextPressureState::new("agent", 32_000, 4_000),
        );
        store.complete(
            "agent-run",
            &report,
            &[],
            "agent-terminal:agent-run".to_string(),
        );
        assert_eq!(
            store.execution_live("agent-run").unwrap().status,
            ExecutionLiveStatus::Complete
        );
        assert_eq!(
            store
                .session_execution_index("session-team")
                .active_execution_ids,
            vec!["root-execution"]
        );
    }

    #[test]
    fn cumulative_inline_root_and_descendant_usage_is_replaced_by_id_without_double_counting() {
        fn telemetry(
            input_tokens: u64,
            output_tokens: u64,
            cache_create_tokens: u64,
            cache_read_tokens: u64,
        ) -> crate::RunModelTelemetry {
            crate::RunModelTelemetry {
                model: Some("model-a".to_string()),
                models_used: vec!["model-a".to_string()],
                first_token_latency_ms: Some(1),
                active_stream_duration_ms: Some(2),
                wall_duration_ms: 3,
                output_chars: output_tokens,
                output_chunks: 1,
                input_tokens,
                output_tokens,
                cache_create_tokens,
                cache_read_tokens,
                cache_dimensions_known: true,
                total_tokens: input_tokens
                    .saturating_add(output_tokens)
                    .saturating_add(cache_create_tokens)
                    .saturating_add(cache_read_tokens),
                usage_source: "provider_actual".to_string(),
                wall_chars_per_second: None,
                wall_tokens_per_second: None,
                active_chars_per_second: None,
                active_tokens_per_second: None,
                chars_per_second: None,
                tokens_per_second: None,
            }
        }

        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        let session_id = "session-model-tree";
        let turn_id = "turn-model-tree";
        let root_execution_id = "root-model-tree";
        let child_execution_id = "child-model-tree";
        store.record_queued(
            session_id,
            root_execution_id.to_string(),
            turn_id.to_string(),
        );

        let root_context = CowdExecutionContext {
            execution_id: root_execution_id.to_string(),
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
        };
        // Each execution emits cumulative snapshots. A later snapshot replaces
        // the prior value for that execution rather than adding another bill.
        for usage in [telemetry(100, 10, 20, 30), telemetry(120, 12, 25, 35)] {
            store.observe_event(
                session_id,
                &CowdEvent::ExecutionScoped {
                    context: root_context.clone(),
                    activity_binding: None,
                    event: Box::new(CowdEvent::RunModelTelemetry { telemetry: usage }),
                },
            );
        }

        let lineage = crate::CowdExecutionLineage {
            parent_execution_id: root_execution_id.to_string(),
            graph_id: "team-graph".to_string(),
            node_id: "researcher:1".to_string(),
            team_id: Some("team-run".to_string()),
            agent_id: Some("researcher".to_string()),
        };
        let child_context = CowdExecutionContext {
            execution_id: child_execution_id.to_string(),
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
        };
        for usage in [telemetry(40, 4, 5, 10), telemetry(50, 5, 7, 13)] {
            store.observe_event(
                session_id,
                &CowdEvent::RelatedExecution {
                    lineage: lineage.clone(),
                    event: Box::new(CowdEvent::ExecutionScoped {
                        context: child_context.clone(),
                        activity_binding: None,
                        event: Box::new(CowdEvent::RunModelTelemetry { telemetry: usage }),
                    }),
                },
            );
        }

        let assert_usage = |live: ExecutionLiveState| {
            assert_eq!(live.metrics.input_tokens, 170);
            assert_eq!(live.metrics.output_tokens, 17);
            assert_eq!(live.metrics.cache_creation_input_tokens, 32);
            assert_eq!(live.metrics.cache_read_input_tokens, 48);
            assert!(live.metrics.cache_dimensions_known);
            assert_eq!(live.metrics.total_tokens, 267);
        };
        assert_usage(store.execution_live(root_execution_id).unwrap());

        // Terminal checkpoint serialization is the durable projection replay
        // path used after a process restart.
        let report = ContextTurnReport::new(
            turn_id,
            harness_contract::context::ContextPressureState::new("default", 32_000, 170),
        );
        store.complete(
            root_execution_id,
            &report,
            &[],
            "terminal:model-tree".to_string(),
        );
        let rehydrated = ExecutionLiveStore::new(event_store);
        assert_usage(rehydrated.execution_live(root_execution_id).unwrap());
    }

    #[test]
    fn durable_model_usage_generation_survives_restart_and_rejects_old_lease() {
        fn telemetry(input_tokens: u64) -> crate::RunModelTelemetry {
            crate::RunModelTelemetry {
                model: Some("model-a".to_string()),
                models_used: vec!["model-a".to_string()],
                first_token_latency_ms: Some(1),
                active_stream_duration_ms: Some(2),
                wall_duration_ms: 3,
                output_chars: 1,
                output_chunks: 1,
                input_tokens,
                output_tokens: 1,
                cache_create_tokens: 0,
                cache_read_tokens: 0,
                cache_dimensions_known: true,
                total_tokens: input_tokens.saturating_add(1),
                usage_source: "provider_actual".to_string(),
                wall_chars_per_second: None,
                wall_tokens_per_second: None,
                active_chars_per_second: None,
                active_tokens_per_second: None,
                chars_per_second: None,
                tokens_per_second: None,
            }
        }

        let event_store = Arc::new(RuntimeEventStore::for_test());
        let session_id = "session-generation-restart";
        let execution_id = "execution-generation-restart";
        let turn_id = "turn-generation-restart";
        let first_process = ExecutionLiveStore::new(Arc::clone(&event_store));
        first_process.record_queued(session_id, execution_id.to_string(), turn_id.to_string());
        let mut old_generation = 0;
        for _ in 0..32 {
            old_generation = first_process
                .allocate_model_usage_generation(execution_id)
                .unwrap();
        }
        drop(first_process);

        let restarted = ExecutionLiveStore::new(event_store);
        let new_generation = restarted
            .allocate_model_usage_generation(execution_id)
            .unwrap();
        assert_eq!(new_generation, old_generation + 1);
        let context = CowdExecutionContext {
            execution_id: execution_id.to_string(),
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
        };
        let scoped = |usage| CowdEvent::ExecutionScoped {
            context: context.clone(),
            activity_binding: None,
            event: Box::new(CowdEvent::RunModelTelemetry { telemetry: usage }),
        };
        restarted.observe_event_with_generation(
            session_id,
            &scoped(telemetry(200)),
            new_generation,
        );
        restarted.observe_event_with_generation(
            session_id,
            &scoped(telemetry(999)),
            old_generation,
        );
        assert_eq!(
            restarted
                .model_usage_snapshot(execution_id)
                .and_then(|snapshot| snapshot.own)
                .map(|usage| usage.input_tokens),
            Some(200)
        );
    }

    #[test]
    fn stale_event_checkpoint_cannot_rollback_generation_between_cas_and_resident_publish() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let session_id = "session-generation-interleave";
        let execution_id = "execution-generation-interleave";
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        store.record_queued(
            session_id,
            execution_id.to_string(),
            "turn-generation-interleave".to_string(),
        );
        let old_generation = store.allocate_model_usage_generation(execution_id).unwrap();
        let shard = store.record_shard(execution_id);
        let mut stale_event_clone = store.record_shards[shard]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(execution_id)
            .unwrap()
            .clone();
        // Model an unrelated event clone that was reduced before allocation,
        // then received a newer visible revision while the allocation CAS was
        // in flight. The hook deterministically persists it after the durable
        // CAS and before the allocator can update the resident record.
        stale_event_clone.live.revision = stale_event_clone.live.revision.saturating_add(100);
        let new_generation = store
            .allocate_model_usage_generation_with_post_cas(execution_id, |_| {
                store.persist(&stale_event_clone).unwrap();
            })
            .unwrap();
        assert_eq!(new_generation, old_generation + 1);
        drop(store);

        let restarted = ExecutionLiveStore::new(Arc::clone(&event_store));
        let after_restart = restarted
            .allocate_model_usage_generation(execution_id)
            .unwrap();
        assert_eq!(after_restart, new_generation + 1);

        let context = CowdExecutionContext {
            execution_id: execution_id.to_string(),
            session_id: session_id.to_string(),
            turn_id: "turn-generation-interleave".to_string(),
        };
        let telemetry = |input_tokens| CowdEvent::ExecutionScoped {
            context: context.clone(),
            activity_binding: None,
            event: Box::new(CowdEvent::RunModelTelemetry {
                telemetry: crate::RunModelTelemetry {
                    model: Some("model-a".to_string()),
                    models_used: vec!["model-a".to_string()],
                    first_token_latency_ms: Some(1),
                    active_stream_duration_ms: Some(2),
                    wall_duration_ms: 3,
                    output_chars: 1,
                    output_chunks: 1,
                    input_tokens,
                    output_tokens: 1,
                    cache_create_tokens: 0,
                    cache_read_tokens: 0,
                    cache_dimensions_known: true,
                    total_tokens: input_tokens.saturating_add(1),
                    usage_source: "provider_actual".to_string(),
                    wall_chars_per_second: None,
                    wall_tokens_per_second: None,
                    active_chars_per_second: None,
                    active_tokens_per_second: None,
                    chars_per_second: None,
                    tokens_per_second: None,
                },
            }),
        };
        restarted.observe_event_with_generation(session_id, &telemetry(200), after_restart);
        restarted.observe_event_with_generation(session_id, &telemetry(999), old_generation);
        assert_eq!(
            restarted
                .model_usage_snapshot(execution_id)
                .and_then(|snapshot| snapshot.own)
                .map(|usage| usage.input_tokens),
            Some(200)
        );
    }

    #[test]
    fn stale_checkpoint_none_cannot_remove_a_durable_model_usage_fence() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let execution_id = "execution-generation-fence";
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        store.record_queued(
            "session-generation-fence",
            execution_id.to_string(),
            "turn-generation-fence".to_string(),
        );
        let generation = store.allocate_model_usage_generation(execution_id).unwrap();
        let shard = store.record_shard(execution_id);
        let mut fenced = store.record_shards[shard]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(execution_id)
            .unwrap()
            .clone();
        fenced.model_usage_fence_generation = Some(generation);
        store.persist(&fenced).unwrap();

        let mut stale = fenced;
        stale.model_usage_fence_generation = None;
        stale.live.revision = stale.live.revision.saturating_add(1);
        store.persist(&stale).unwrap();
        drop(store);

        let checkpoint = event_store
            .projection_checkpoint(&live_projection_id(execution_id))
            .unwrap()
            .unwrap();
        let recovered: LiveExecutionRecord = serde_json::from_value(checkpoint.payload).unwrap();
        assert_eq!(
            recovered.model_usage_fence_generation,
            Some(generation),
            "a newer stale clone must not turn a durable Some fence back into None"
        );
    }

    #[test]
    fn synchronous_telemetry_precedes_surface_relay_and_relay_replay_does_not_double_count() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = Arc::new(ExecutionLiveStore::new(event_store));
        let session_id = "session-sync-telemetry";
        let root_execution_id = "root-sync-telemetry";
        let child_execution_id = "child-sync-telemetry";
        store.record_queued(
            session_id,
            root_execution_id.to_string(),
            "turn-sync-telemetry".to_string(),
        );

        let root_bus = crate::CowdEventBus::new();
        let observer_store = Arc::clone(&store);
        root_bus.bind_live_telemetry_observer(1, move |event: &crate::CowdEvent| {
            let context = event.execution_context().expect("scoped telemetry");
            observer_store.observe_event_with_generation(&context.session_id, event, 1);
        });
        let mut surface_relay = root_bus.subscribe();
        let _root_scope = root_bus.enter_execution(CowdExecutionContext {
            execution_id: root_execution_id.to_string(),
            session_id: session_id.to_string(),
            turn_id: "turn-sync-telemetry".to_string(),
        });

        let child_bus = crate::CowdEventBus::new();
        child_bus.forward_to(
            &root_bus,
            crate::CowdExecutionLineage {
                parent_execution_id: root_execution_id.to_string(),
                graph_id: "team-sync-telemetry".to_string(),
                node_id: "agent:1".to_string(),
                team_id: Some("team-1".to_string()),
                agent_id: Some("agent-1".to_string()),
            },
        );
        let _child_scope = child_bus.enter_execution(CowdExecutionContext {
            execution_id: child_execution_id.to_string(),
            session_id: session_id.to_string(),
            turn_id: "turn-sync-telemetry".to_string(),
        });
        let child_usage = crate::RunModelTelemetry {
            model: Some("model-a".to_string()),
            models_used: vec!["model-a".to_string()],
            first_token_latency_ms: Some(1),
            active_stream_duration_ms: Some(2),
            wall_duration_ms: 3,
            output_chars: 5,
            output_chunks: 1,
            input_tokens: 20,
            output_tokens: 5,
            cache_create_tokens: 0,
            cache_read_tokens: 0,
            cache_dimensions_known: true,
            total_tokens: 25,
            usage_source: "provider_actual".to_string(),
            wall_chars_per_second: None,
            wall_tokens_per_second: None,
            active_chars_per_second: None,
            active_tokens_per_second: None,
            chars_per_second: None,
            tokens_per_second: None,
        };
        child_bus.emit(CowdEvent::RunModelTelemetry {
            telemetry: child_usage,
        });

        // The store is already complete before the asynchronous Surface relay
        // receives the same event.
        let snapshot = store.model_usage_snapshot(root_execution_id).unwrap();
        assert!(snapshot.own.is_none());
        assert_eq!(snapshot.descendants.len(), 1);
        assert_eq!(
            store
                .execution_live(root_execution_id)
                .unwrap()
                .metrics
                .input_tokens,
            20
        );

        let relayed = surface_relay.try_recv().expect("related telemetry relay");
        store.observe_event(session_id, &relayed);
        let after_relay = store.execution_live(root_execution_id).unwrap();
        assert_eq!(after_relay.metrics.input_tokens, 20);
        assert_eq!(after_relay.metrics.output_tokens, 5);
        assert!(after_relay.metrics.cache_dimensions_known);
    }

    #[test]
    fn terminal_usage_fence_rejects_late_child_telemetry_in_hot_and_replay_state() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        let session_id = "session-terminal-usage-fence";
        let turn_id = "turn-terminal-usage-fence";
        let root_execution_id = "root-terminal-usage-fence";
        let child_execution_id = "child-terminal-usage-fence";
        store.record_queued(
            session_id,
            root_execution_id.to_string(),
            turn_id.to_string(),
        );
        let telemetry = |input_tokens| crate::RunModelTelemetry {
            model: Some("model-a".to_string()),
            models_used: vec!["model-a".to_string()],
            first_token_latency_ms: Some(1),
            active_stream_duration_ms: Some(2),
            wall_duration_ms: 3,
            output_chars: 1,
            output_chunks: 1,
            input_tokens,
            output_tokens: 1,
            cache_create_tokens: 2,
            cache_read_tokens: 3,
            cache_dimensions_known: true,
            total_tokens: input_tokens + 6,
            usage_source: "provider_actual".to_string(),
            wall_chars_per_second: None,
            wall_tokens_per_second: None,
            active_chars_per_second: None,
            active_tokens_per_second: None,
            chars_per_second: None,
            tokens_per_second: None,
        };
        let related = |usage| CowdEvent::RelatedExecution {
            lineage: crate::CowdExecutionLineage {
                parent_execution_id: root_execution_id.to_string(),
                graph_id: "team-terminal-fence".to_string(),
                node_id: "agent:1".to_string(),
                team_id: Some("team-1".to_string()),
                agent_id: Some("agent-1".to_string()),
            },
            event: Box::new(CowdEvent::ExecutionScoped {
                context: CowdExecutionContext {
                    execution_id: child_execution_id.to_string(),
                    session_id: session_id.to_string(),
                    turn_id: turn_id.to_string(),
                },
                activity_binding: None,
                event: Box::new(CowdEvent::RunModelTelemetry { telemetry: usage }),
            }),
        };
        store.observe_event_with_generation(session_id, &related(telemetry(20)), 7);
        let report = ContextTurnReport::new(
            turn_id,
            harness_contract::context::ContextPressureState::new("default", 32_000, 20),
        );
        store.complete(
            root_execution_id,
            &report,
            &[],
            "terminal:usage-fence".to_string(),
        );
        let sealed = store.execution_live(root_execution_id).unwrap();
        assert_eq!(sealed.metrics.input_tokens, 20);
        let sealed_revision = sealed.revision;

        // A child event emitted by the old execution bus after terminal seal
        // may still reach an asynchronous relay. It cannot change the root's
        // hot or durable terminal bill.
        store.observe_event_with_generation(session_id, &related(telemetry(200)), 7);
        let after_late = store.execution_live(root_execution_id).unwrap();
        assert_eq!(after_late.metrics.input_tokens, 20);
        assert_eq!(after_late.revision, sealed_revision);

        let replayed = ExecutionLiveStore::new(event_store)
            .execution_live(root_execution_id)
            .unwrap();
        assert_eq!(replayed.metrics.input_tokens, 20);
        assert_eq!(replayed.revision, sealed_revision);
    }

    #[test]
    fn scoped_event_updates_only_its_execution_and_rehydrates_from_mutable_projection() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        store.record_queued("session-a", "execution-a".to_string(), "turn-a".to_string());
        store.record_queued("session-a", "execution-b".to_string(), "turn-b".to_string());
        store.observe_event(
            "session-a",
            &CowdEvent::ExecutionScoped {
                context: CowdExecutionContext {
                    execution_id: "execution-a".to_string(),
                    session_id: "session-a".to_string(),
                    turn_id: "turn-a".to_string(),
                },
                activity_binding: None,
                event: Box::new(CowdEvent::ExecutionPhase {
                    status: ExecutionLiveStatus::CallingModel,
                    detail: Some("requesting model".to_string()),
                }),
            },
        );
        assert_eq!(
            store.execution_live("execution-a").unwrap().status,
            ExecutionLiveStatus::CallingModel
        );
        assert_eq!(
            store.execution_live("execution-b").unwrap().status,
            ExecutionLiveStatus::Queued
        );

        let rehydrated = ExecutionLiveStore::new(Arc::clone(&event_store));
        assert_eq!(
            rehydrated.execution_live("execution-a").unwrap().status,
            ExecutionLiveStatus::CallingModel
        );
        let checkpoint = event_store
            .projection_checkpoint(&live_projection_id("execution-a"))
            .unwrap()
            .expect("live checkpoint");
        assert_eq!(
            checkpoint.source_cursor, 0,
            "live revision and canonical journal cursor are independent"
        );
        assert_eq!(checkpoint.revision, 2);
        assert_eq!(
            event_store.all_events(10).unwrap().len(),
            0,
            "derived live state must not enter the immutable journal"
        );
    }

    #[test]
    fn restart_advances_canonical_cursor_without_granting_business_status_lifecycle_authority() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let execution_id = "execution-cursor-replay";
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        store.record_queued(
            "session-cursor-replay",
            execution_id.to_string(),
            "turn-cursor-replay".to_string(),
        );
        let checkpoint = event_store
            .projection_checkpoint(&live_projection_id(execution_id))
            .unwrap()
            .expect("queued checkpoint");
        assert_eq!(checkpoint.source_cursor, 0);

        event_store
            .append(
                RuntimeEventInput {
                    stream_id: execution_id.to_string(),
                    scope: RuntimeEventScope::ExecutionGraph,
                    kind: "execution_graph.delta.v1".to_string(),
                    status: Some("waiting_approval".to_string()),
                    actor: Some("test".to_string()),
                    refs: Vec::new(),
                    payload: serde_json::json!({"revision": 1}),
                }
                .with_activity_binding(harness_contract::projection::RuntimeActivityBinding {
                    root_execution_id: execution_id.to_string(),
                    session_id: "session-cursor-replay".to_string(),
                    turn_id: "turn-cursor-replay".to_string(),
                    root_task_id: "task-cursor-replay".to_string(),
                    task_id: "task-cursor-replay".to_string(),
                    activity_id: format!("activity:execution:{execution_id}"),
                    node_id: None,
                    parent_activity_id: None,
                    initiator_activity_id: None,
                    team_run_id: None,
                    agent_instance_id: None,
                    agent_run_id: None,
                    skill_id: None,
                    skill_revision: None,
                    skill_activation_id: None,
                    tool_contract_id: None,
                    tool_call_id: None,
                    approval_id: Some("approval-cursor-replay".to_string()),
                    parallel_group_id: None,
                    revision: 1,
                    fence: 1,
                    generation: 1,
                })
                .unwrap(),
            )
            .unwrap();

        let recovered = ExecutionLiveStore::new(Arc::clone(&event_store))
            .execution_live(execution_id)
            .expect("active checkpoint recovered");
        assert_eq!(
            recovered.status,
            ExecutionLiveStatus::Queued,
            "a graph/business status cannot mutate the owning live execution lifecycle"
        );
        assert_eq!(
            event_store
                .projection_checkpoint(&live_projection_id(execution_id))
                .unwrap()
                .expect("advanced checkpoint")
                .source_cursor,
            1
        );
    }

    #[test]
    fn completed_child_event_cannot_precomplete_agent_before_finalizing() {
        let mut record = LiveExecutionRecord::new(
            "session-child-complete".to_string(),
            "agent-run-child-complete".to_string(),
            "turn-child-complete".to_string(),
        );
        assert!(record.transition(
            ExecutionLiveStatus::CallingModel,
            Some("agent model running".to_string())
        ));
        record.apply_durable_event(&DurableRuntimeEvent {
            event_id: "child-node-completed".to_string(),
            stream_id: "team-graph:child".to_string(),
            sequence: 1,
            scope: crate::RuntimeEventScope::ExecutionNode,
            kind: "execution_node.transitioned".to_string(),
            status: Some("completed".to_string()),
            actor: Some("execution_commit_service".to_string()),
            refs: vec![crate::RuntimeEventRef {
                kind: "agent_run".to_string(),
                id: record.execution_id.clone(),
            }],
            payload: serde_json::json!({"node_id": "investigator:1"}),
            created_at_ms: current_time_ms(),
            commit_cursor: 1,
            transaction_id: "child-node-completed-tx".to_string(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: Some("child-node-completed-key".to_string()),
        });

        assert_eq!(record.live.status, ExecutionLiveStatus::CallingModel);
        assert!(record.transition(
            ExecutionLiveStatus::Finalizing,
            Some("synthesizing terminal".to_string())
        ));
    }

    #[test]
    fn generic_durable_status_vocabulary_never_owns_live_lifecycle() {
        for (index, (kind, status)) in [
            ("execution_graph.command_applied", "completed"),
            ("tool.invocation.failed", "failed"),
            ("knowledge.candidate.projector.failed.v1", "blocked"),
            ("execution_graph.delta.v1", "cancelled"),
            ("execution_node.transitioned", "waiting_approval"),
        ]
        .into_iter()
        .enumerate()
        {
            let mut record = LiveExecutionRecord::new(
                format!("session-generic-status-{index}"),
                format!("execution-generic-status-{index}"),
                format!("turn-generic-status-{index}"),
            );
            assert!(record.transition(
                ExecutionLiveStatus::CallingModel,
                Some("agent model running".to_string())
            ));
            record.apply_durable_event(&DurableRuntimeEvent {
                event_id: format!("generic-status-{index}"),
                stream_id: format!("nested-stream-{index}"),
                sequence: 1,
                scope: crate::RuntimeEventScope::ExecutionGraph,
                kind: kind.to_string(),
                status: Some(status.to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"reason": "nested business outcome"}),
                created_at_ms: current_time_ms(),
                commit_cursor: (index + 1).try_into().unwrap(),
                transaction_id: format!("generic-status-tx-{index}"),
                transaction_index: 0,
                schema_version: 1,
                idempotency_key: Some(format!("generic-status-key-{index}")),
            });
            assert_eq!(
                record.live.status,
                ExecutionLiveStatus::CallingModel,
                "{kind} status={status} stole live lifecycle authority"
            );
        }
    }

    #[test]
    fn terminal_delivery_request_never_owns_live_completion() {
        let mut record = LiveExecutionRecord::new(
            "session-explicit-terminal".to_string(),
            "execution-explicit-terminal".to_string(),
            "turn-explicit-terminal".to_string(),
        );
        assert!(record.transition(
            ExecutionLiveStatus::CallingModel,
            Some("agent model running".to_string())
        ));
        assert!(record.transition(
            ExecutionLiveStatus::Finalizing,
            Some("synthesizing terminal".to_string())
        ));
        let terminal = DurableRuntimeEvent {
            event_id: "terminal-requested".to_string(),
            stream_id: "session-terminal:input".to_string(),
            sequence: 1,
            scope: crate::RuntimeEventScope::SessionInput,
            kind: "runtime.session.terminal_requested".to_string(),
            status: Some("pending_delivery".to_string()),
            actor: Some("conversation_runtime".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"payload_ref": "terminal:durable"}),
            created_at_ms: current_time_ms(),
            commit_cursor: 2,
            transaction_id: "terminal-requested-tx".to_string(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: Some("terminal-requested-key".to_string()),
        };

        let mut malformed = terminal.clone();
        malformed.payload = serde_json::json!({});
        record.apply_durable_event(&malformed);
        assert_eq!(record.live.status, ExecutionLiveStatus::Finalizing);

        record.apply_durable_event(&terminal);
        assert_eq!(record.live.status, ExecutionLiveStatus::Finalizing);
        assert_eq!(record.live.terminal_ref, None);
        let revision = record.live.revision;
        record.apply_durable_event(&terminal);
        assert_eq!(record.live.status, ExecutionLiveStatus::Finalizing);
        assert_eq!(record.live.terminal_ref, None);
        assert_eq!(record.live.revision, revision);
        assert!(record.pending_terminal_claim.is_none());
    }

    #[test]
    fn finalizing_remains_recoverable_until_a_durable_terminal_commit() {
        let mut record = LiveExecutionRecord::new(
            "session-finalizing-replan".to_string(),
            "execution-finalizing-replan".to_string(),
            "turn-finalizing-replan".to_string(),
        );
        assert!(record.transition(
            ExecutionLiveStatus::CallingModel,
            Some("drafting terminal candidate".to_string())
        ));
        assert!(record.transition(
            ExecutionLiveStatus::Finalizing,
            Some("synthesizing terminal".to_string())
        ));
        assert!(record.transition(
            ExecutionLiveStatus::PreparingContext,
            Some("terminal candidate superseded".to_string())
        ));
        assert!(record.transition(
            ExecutionLiveStatus::CallingModel,
            Some("terminal quality retry".to_string())
        ));
        assert_eq!(record.live.status, ExecutionLiveStatus::CallingModel);
    }

    #[test]
    fn high_frequency_live_updates_coalesce_until_a_lifecycle_boundary() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        let context = CowdExecutionContext {
            execution_id: "execution-coalesced".to_string(),
            session_id: "session-coalesced".to_string(),
            turn_id: "turn-coalesced".to_string(),
        };
        store.record_queued(
            &context.session_id,
            context.execution_id.clone(),
            context.turn_id.clone(),
        );
        let queued_checkpoint = event_store
            .projection_checkpoint(&live_projection_id(&context.execution_id))
            .unwrap()
            .expect("queued checkpoint");

        for index in 0..8 {
            store.observe_event(
                &context.session_id,
                &CowdEvent::ExecutionScoped {
                    context: context.clone(),
                    activity_binding: None,
                    event: Box::new(CowdEvent::ToolStart {
                        id: format!("tool-{index}"),
                        name: "read_file".to_string(),
                        preview: format!("file-{index}.md"),
                    }),
                },
            );
        }
        let hot = store
            .execution_live(&context.execution_id)
            .expect("hot record");
        assert_eq!(hot.metrics.tool_calls, 8);
        let queued: LiveExecutionRecord =
            serde_json::from_value(queued_checkpoint.payload.clone()).unwrap();
        assert!(hot.revision > queued.live.revision);
        assert_eq!(
            event_store
                .projection_checkpoint(&live_projection_id(&context.execution_id))
                .unwrap()
                .expect("coalesced checkpoint")
                .source_cursor,
            queued_checkpoint.source_cursor,
            "sub-threshold updates must remain hot instead of amplifying storage writes"
        );

        store.observe_event(
            &context.session_id,
            &CowdEvent::ExecutionScoped {
                context: context.clone(),
                activity_binding: None,
                event: Box::new(CowdEvent::ExecutionPhase {
                    status: ExecutionLiveStatus::CallingModel,
                    detail: Some("model boundary".to_string()),
                }),
            },
        );
        let durable = event_store
            .projection_checkpoint(&live_projection_id(&context.execution_id))
            .unwrap()
            .expect("boundary checkpoint");
        assert_eq!(durable.source_cursor, 0);
        assert_eq!(durable.revision, queued_checkpoint.revision + 1);
        let rehydrated = ExecutionLiveStore::new(Arc::clone(&event_store));
        let recovered = rehydrated
            .execution_live(&context.execution_id)
            .expect("recovered boundary record");
        assert_eq!(recovered.metrics.tool_calls, 8);
        assert_eq!(recovered.status, ExecutionLiveStatus::CallingModel);
        assert!(event_store.all_events(10).unwrap().is_empty());
    }

    #[test]
    fn live_output_recovery_uses_runtime_projection_identity_across_text_items() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(event_store);
        let context = CowdExecutionContext {
            execution_id: "execution-output".to_string(),
            session_id: "session-output".to_string(),
            turn_id: "turn-output".to_string(),
        };
        store.record_queued(
            &context.session_id,
            context.execution_id.clone(),
            context.turn_id.clone(),
        );
        for (sequence, item_id, text) in [(1, "text-a", "first "), (2, "text-b", "second")] {
            let identity = crate::CausalItemIdentity {
                model_step_id: format!("{}:model-step:{sequence}", context.execution_id),
                item_id: item_id.to_string(),
                segment_id: format!("{item_id}:text:0"),
                causal_sequence: sequence,
                delta_sequence: 1,
                tool_call_id: None,
                causal_parent_ids: Vec::new(),
            };
            store.observe_event(
                &context.session_id,
                &CowdEvent::ExecutionScoped {
                    context: context.clone(),
                    activity_binding: None,
                    event: Box::new(CowdEvent::Causal {
                        identity: identity.clone(),
                        event: Box::new(CowdEvent::TextDelta {
                            text: text.to_string(),
                        }),
                    }),
                },
            );
            store.observe_event(
                &context.session_id,
                &CowdEvent::ExecutionScoped {
                    context: context.clone(),
                    activity_binding: None,
                    event: Box::new(CowdEvent::Causal {
                        identity: crate::CausalItemIdentity {
                            delta_sequence: 2,
                            ..identity
                        },
                        event: Box::new(CowdEvent::ItemCompleted {
                            kind: crate::CausalItemKind::Text,
                            tool_name: None,
                            tool_input: None,
                        }),
                    }),
                },
            );
        }
        let live = store.execution_live(&context.execution_id).unwrap();
        assert_eq!(live.output_preview.as_deref(), Some("first second"));
        assert_eq!(live.output_bytes, 12);
        assert_eq!(live.output_parts.len(), 2);
        assert_eq!(live.output_parts[0].part_id, "text-a:text:0");
        assert_eq!(live.output_parts[0].bytes, 6);
        assert!(live.output_parts[0].completed);
        assert_eq!(live.output_parts[1].part_id, "text-b:text:0");
        assert_eq!(live.output_parts[1].bytes, 6);
        assert!(live.output_parts[1].completed);
    }

    #[test]
    fn approval_metrics_deduplicate_stable_request_ids_across_replay_and_restart() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        let context = CowdExecutionContext {
            execution_id: "execution-approval".to_string(),
            session_id: "session-approval".to_string(),
            turn_id: "turn-approval".to_string(),
        };
        store.record_queued(
            &context.session_id,
            context.execution_id.clone(),
            context.turn_id.clone(),
        );
        let approval = |request_id: &str| CowdEvent::ExecutionScoped {
            context: context.clone(),
            activity_binding: None,
            event: Box::new(CowdEvent::ApprovalRequested {
                request_id: request_id.to_string(),
                tool: "write_file".to_string(),
            }),
        };

        store.observe_event(&context.session_id, &approval("approval-1"));
        store.observe_event(&context.session_id, &approval("approval-1"));
        assert_eq!(
            store
                .execution_live(&context.execution_id)
                .unwrap()
                .metrics
                .approvals,
            1
        );

        let rehydrated = ExecutionLiveStore::new(event_store);
        rehydrated.observe_event(&context.session_id, &approval("approval-1"));
        rehydrated.observe_event(&context.session_id, &approval("approval-2"));
        assert_eq!(
            rehydrated
                .execution_live(&context.execution_id)
                .unwrap()
                .metrics
                .approvals,
            2,
            "restart replay and duplicate delivery must not inflate approval metrics"
        );
    }

    #[test]
    fn hot_cache_prunes_only_terminal_records() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let hot_state = Arc::new(RuntimeHotStatePlane::new(
            crate::execution_core::hot_state::HotStateConfig {
                memory: crate::execution_core::hot_state::HotStateMemoryConfig {
                    max_bytes: Some(4 * 1024),
                    ..Default::default()
                },
                ..Default::default()
            },
        ));
        let store = ExecutionLiveStore::with_hot_state(event_store, hot_state);
        for index in 0..64 {
            let execution_id = format!("terminal-{index}");
            store.record_queued("session-a", execution_id.clone(), format!("turn-{index}"));
            let _ = store.cancel(&execution_id, "complete for cache test".to_string());
        }
        store.record_queued(
            "session-a",
            "active-execution".to_string(),
            "active-turn".to_string(),
        );

        let records = store.all_records();
        assert!(records.len() < 65);
        assert!(records
            .iter()
            .any(|record| record.execution_id == "active-execution"));
    }

    #[test]
    fn cancellation_and_terminal_commit_share_one_terminal_winner() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(event_store);
        let report = ContextTurnReport::new(
            "turn-terminal-race",
            harness_contract::context::ContextPressureState::new("direct", 32_000, 1_000),
        )
        .with_ledger(harness_contract::context::ContextLedgerProjection {
            max_tokens: 32_000,
            consumed_tokens: 1_000,
            remaining_tokens: 31_000,
            tool_result_limit: 0,
            tool_result_consumed: 0,
            components: Vec::new(),
            request_sequence: 2,
            calibrated_input_tokens: Some(900),
        });

        store.record_queued(
            "session-race",
            "cancel-wins".to_string(),
            "turn-cancel-wins".to_string(),
        );
        assert!(store
            .cancel("cancel-wins", "user cancelled".to_string())
            .unwrap());
        assert_eq!(
            store
                .claim_terminal(
                    "cancel-wins",
                    "terminal-too-late".to_string(),
                    ExecutionLiveStatus::Complete,
                    1,
                )
                .unwrap(),
            TerminalFenceClaim::ConflictingTerminal
        );
        let cancelled = store.execution_live("cancel-wins").unwrap();
        assert_eq!(cancelled.status, ExecutionLiveStatus::Cancelled);
        assert!(cancelled.terminal_ref.is_none());

        store.record_queued(
            "session-race",
            "terminal-wins".to_string(),
            "turn-terminal-wins".to_string(),
        );
        assert_eq!(
            store
                .claim_terminal(
                    "terminal-wins",
                    "terminal-committed".to_string(),
                    ExecutionLiveStatus::Complete,
                    2,
                )
                .unwrap(),
            TerminalFenceClaim::Claimed
        );
        assert!(!store
            .cancel("terminal-wins", "late cancel".to_string())
            .unwrap());
        assert!(!store
            .execution_live("terminal-wins")
            .unwrap()
            .status
            .is_terminal());
        assert_eq!(
            store
                .finalize_terminal(
                    "terminal-wins",
                    "terminal-committed",
                    ExecutionLiveStatus::Complete,
                    2,
                )
                .unwrap(),
            TerminalFenceClaim::Claimed
        );
        store
            .enrich_finalized_session_terminal(
                "terminal-wins",
                ExecutionLiveStatus::Complete,
                "terminal-committed".to_string(),
                &report,
                &["result.md".to_string()],
                None,
            )
            .unwrap();
        assert_eq!(
            store.execution_live("terminal-wins").unwrap().status,
            ExecutionLiveStatus::Complete
        );
        assert_eq!(
            store
                .execution_live("terminal-wins")
                .unwrap()
                .metrics
                .files_touched,
            1,
            "same durable terminal must accept the complete post-commit report"
        );
        assert_eq!(
            store
                .execution_live("terminal-wins")
                .unwrap()
                .context_usage
                .and_then(|usage| usage.input_tokens),
            Some(900)
        );
    }

    #[test]
    fn pending_terminal_claim_survives_restart_without_exposing_false_terminal() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        {
            let store = ExecutionLiveStore::new(Arc::clone(&event_store));
            store.record_queued(
                "session-restart",
                "execution-restart".to_string(),
                "turn-restart".to_string(),
            );
            assert_eq!(
                store
                    .claim_terminal(
                        "execution-restart",
                        "terminal-restart".to_string(),
                        ExecutionLiveStatus::Complete,
                        7,
                    )
                    .unwrap(),
                TerminalFenceClaim::Claimed
            );
            let pending = store.execution_live("execution-restart").unwrap();
            assert!(!pending.status.is_terminal());
            assert!(pending.terminal_ref.is_none());
        }

        let recovered = ExecutionLiveStore::new(Arc::clone(&event_store));
        assert_eq!(
            recovered
                .claim_terminal(
                    "execution-restart",
                    "terminal-restart".to_string(),
                    ExecutionLiveStatus::Complete,
                    7,
                )
                .unwrap(),
            TerminalFenceClaim::SamePending
        );
        assert_eq!(
            recovered
                .finalize_terminal(
                    "execution-restart",
                    "terminal-restart",
                    ExecutionLiveStatus::Complete,
                    7,
                )
                .unwrap(),
            TerminalFenceClaim::Claimed
        );
        assert_eq!(
            recovered
                .execution_live("execution-restart")
                .unwrap()
                .terminal_ref
                .as_deref(),
            Some("terminal-restart")
        );
        recovered.release_terminal_checkpoint("execution-restart");
        drop(recovered);
        assert!(ExecutionLiveStore::new(event_store)
            .execution_live("execution-restart")
            .is_none());
    }

    #[test]
    fn deterministic_delivery_failure_aborts_pending_claim_and_reopens_cancellation() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(event_store);
        let execution_id = "execution-abort-pending";
        store.record_queued(
            "session-abort-pending",
            execution_id.to_string(),
            "turn-abort-pending".to_string(),
        );
        assert_eq!(
            store
                .claim_terminal(
                    execution_id,
                    "terminal-stale".to_string(),
                    ExecutionLiveStatus::Complete,
                    3,
                )
                .unwrap(),
            TerminalFenceClaim::Claimed
        );
        assert!(!store
            .cancel(
                execution_id,
                "cancel waits for prepared delivery".to_string()
            )
            .unwrap());
        assert!(store
            .abort_terminal_claim(execution_id, "terminal-stale", 3)
            .unwrap());
        assert!(store
            .cancel(execution_id, "cancel after deterministic abort".to_string())
            .unwrap());
        assert_eq!(
            store.execution_live(execution_id).unwrap().status,
            ExecutionLiveStatus::Cancelled
        );
    }

    #[test]
    fn session_foreground_enrichment_cannot_finalize_or_clear_delivery_claim() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(event_store);
        let execution_id = "execution-enrichment-fence";
        let terminal_ref = "terminal-enrichment-fence";
        store.record_queued(
            "session-enrichment-fence",
            execution_id.to_string(),
            "turn-enrichment-fence".to_string(),
        );
        assert_eq!(
            store
                .claim_terminal(
                    execution_id,
                    terminal_ref.to_string(),
                    ExecutionLiveStatus::Complete,
                    11,
                )
                .unwrap(),
            TerminalFenceClaim::Claimed
        );
        let report = ContextTurnReport::new(
            "turn-enrichment-fence",
            harness_contract::context::ContextPressureState::new("default", 32_000, 512),
        );

        let error = store
            .enrich_finalized_session_terminal(
                execution_id,
                ExecutionLiveStatus::Complete,
                terminal_ref.to_string(),
                &report,
                &["must-not-appear.md".to_string()],
                None,
            )
            .unwrap_err();
        assert!(error.contains("enrichment rejected"));
        assert!(!store
            .execution_live(execution_id)
            .unwrap()
            .status
            .is_terminal());
        assert_eq!(
            store
                .claim_terminal(
                    execution_id,
                    terminal_ref.to_string(),
                    ExecutionLiveStatus::Complete,
                    11,
                )
                .unwrap(),
            TerminalFenceClaim::SamePending,
            "foreground enrichment must not remove the delivery winner"
        );
        assert!(store
            .abort_terminal_claim(execution_id, terminal_ref, 11)
            .unwrap());
        assert!(!store
            .execution_live(execution_id)
            .unwrap()
            .status
            .is_terminal());
    }

    #[test]
    fn cancelled_winner_without_terminal_ref_survives_requested_crash_window() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        {
            let store = ExecutionLiveStore::new(Arc::clone(&event_store));
            store.record_queued(
                "session-cancel-restart",
                "execution-cancel-restart".to_string(),
                "turn-cancel-restart".to_string(),
            );
            assert!(store
                .cancel(
                    "execution-cancel-restart",
                    "user cancellation won".to_string(),
                )
                .unwrap());
        }
        assert_eq!(
            ExecutionLiveStore::new(event_store)
                .execution_live("execution-cancel-restart")
                .unwrap()
                .status,
            ExecutionLiveStatus::Cancelled
        );
    }

    #[test]
    fn stale_nonterminal_checkpoint_cannot_overwrite_or_resurrect_terminal_winner() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        let execution_id = "execution-stale-checkpoint";
        store.record_queued(
            "session-stale-checkpoint",
            execution_id.to_string(),
            "turn-stale-checkpoint".to_string(),
        );
        let stale = store
            .all_records()
            .into_iter()
            .find(|record| record.execution_id == execution_id)
            .unwrap();
        assert!(store.cancel(execution_id, "winner".to_string()).unwrap());
        assert!(
            store.persist(&stale).is_ok(),
            "stale persistence is a no-op"
        );
        assert_eq!(
            ExecutionLiveStore::new(Arc::clone(&event_store))
                .execution_live(execution_id)
                .unwrap()
                .status,
            ExecutionLiveStatus::Cancelled
        );

        store.release_terminal_checkpoint(execution_id);
        assert!(
            store.persist(&stale).is_ok(),
            "released checkpoint rejects resurrection"
        );
        assert!(event_store
            .projection_checkpoint(&live_projection_id(execution_id))
            .unwrap()
            .is_none());
    }

    #[test]
    fn completion_projects_real_memory_recall_occurrences_from_the_context_ledger() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(event_store);
        store.record_queued(
            "session-a",
            "execution-memory".to_string(),
            "turn-memory".to_string(),
        );
        let report = ContextTurnReport::new(
            "turn-memory",
            harness_contract::context::ContextPressureState::new("default", 32_000, 1_000),
        )
        .with_ledger(harness_contract::context::ContextLedgerProjection {
            max_tokens: 32_000,
            consumed_tokens: 1_000,
            remaining_tokens: 31_000,
            tool_result_limit: 0,
            tool_result_consumed: 0,
            components: vec![harness_contract::context::ContextComponentUsage {
                kind: "memory".to_string(),
                tokens: 80,
                occurrences: 3,
            }],
            request_sequence: 1,
            calibrated_input_tokens: Some(900),
        });

        store.complete(
            "execution-memory",
            &report,
            &[],
            "terminal-memory".to_string(),
        );
        assert_eq!(
            store
                .execution_live("execution-memory")
                .unwrap()
                .metrics
                .memory_recalls,
            3
        );
    }

    #[test]
    fn generic_evidence_audits_are_not_relabelled_as_memory_evidence() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(event_store);
        store.record_queued(
            "session-a",
            "execution-workspace-evidence".to_string(),
            "turn-workspace-evidence".to_string(),
        );
        let report = ContextTurnReport::new(
            "turn-workspace-evidence",
            harness_contract::context::ContextPressureState::new("default", 32_000, 100),
        )
        .with_audit_projection(harness_contract::context::EvidenceAuditProjection {
            evidence_ref: harness_contract::context::EvidenceRef::observed(
                "workspace_file",
                "src/lib.rs",
            ),
            content_kind: harness_contract::context::EvidenceContentKind::Text,
            raw_tokens: 100,
            receipt_tokens: 10,
            omitted_tokens: 90,
            raw_available: true,
            access: None,
        });

        store.complete(
            "execution-workspace-evidence",
            &report,
            &[],
            "terminal-workspace-evidence".to_string(),
        );
        let metrics = store
            .execution_live("execution-workspace-evidence")
            .unwrap()
            .metrics;
        assert_eq!(metrics.memory_evidence, 0);
        assert_eq!(metrics.memory_recalls, 0);
    }

    #[test]
    fn cumulative_model_telemetry_does_not_overwrite_request_context_occupancy() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(event_store);
        let execution_id = "execution-multi-step";
        let context = CowdExecutionContext {
            execution_id: execution_id.to_string(),
            session_id: "session-multi-step".to_string(),
            turn_id: "turn-multi-step".to_string(),
        };
        store.record_queued(
            &context.session_id,
            execution_id.to_string(),
            context.turn_id.clone(),
        );
        store.observe_event(
            &context.session_id,
            &CowdEvent::ExecutionScoped {
                context: context.clone(),
                activity_binding: None,
                event: Box::new(CowdEvent::ProviderAttempt {
                    model: "model-a".to_string(),
                    models_tried: vec!["model-a".to_string()],
                    context_window_tokens: 32_000,
                    context_window_source: "provider".to_string(),
                    packed_input_tokens: 700,
                }),
            },
        );
        store.observe_event(
            &context.session_id,
            &CowdEvent::ExecutionScoped {
                context: context.clone(),
                activity_binding: None,
                event: Box::new(CowdEvent::RunModelTelemetry {
                    telemetry: crate::RunModelTelemetry {
                        model: Some("model-a".to_string()),
                        models_used: vec!["model-a".to_string()],
                        first_token_latency_ms: Some(1),
                        active_stream_duration_ms: Some(2),
                        wall_duration_ms: 3,
                        output_chars: 10,
                        output_chunks: 2,
                        input_tokens: 1_300,
                        output_tokens: 40,
                        cache_create_tokens: 0,
                        cache_read_tokens: 0,
                        cache_dimensions_known: true,
                        total_tokens: 1_340,
                        usage_source: "provider_actual".to_string(),
                        wall_chars_per_second: None,
                        wall_tokens_per_second: None,
                        active_chars_per_second: None,
                        active_tokens_per_second: None,
                        chars_per_second: None,
                        tokens_per_second: None,
                    },
                }),
            },
        );

        let live = store.execution_live(execution_id).unwrap();
        assert_eq!(live.metrics.input_tokens, 1_300);
        assert_eq!(live.latency.provider_wall_ms, 3);
        assert_eq!(live.latency.first_token_latency_ms, Some(1));
        assert_eq!(live.latency.provider_active_stream_ms, 2);
        assert_eq!(
            live.latency.total_elapsed_ms,
            live.latency
                .harness_elapsed_ms
                .saturating_add(live.latency.provider_wall_ms)
        );
        assert_eq!(
            live.context_usage.and_then(|usage| usage.input_tokens),
            Some(700),
            "context occupancy is the latest packed request, not cumulative billed input"
        );
    }

    #[test]
    fn terminal_ledger_keeps_provider_model_window_and_calibrates_actual_input() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(event_store);
        let execution_id = "execution-context-authority";
        let context = CowdExecutionContext {
            execution_id: execution_id.to_string(),
            session_id: "session-context-authority".to_string(),
            turn_id: "turn-context-authority".to_string(),
        };
        store.record_queued(
            &context.session_id,
            execution_id.to_string(),
            context.turn_id.clone(),
        );
        store.observe_event(
            &context.session_id,
            &CowdEvent::ExecutionScoped {
                context: context.clone(),
                activity_binding: None,
                event: Box::new(CowdEvent::ProviderAttempt {
                    model: "provider-model".to_string(),
                    models_tried: vec!["provider-model".to_string()],
                    context_window_tokens: 128_000,
                    context_window_source: "registry".to_string(),
                    packed_input_tokens: 8_000,
                }),
            },
        );
        let report = ContextTurnReport::new(
            "turn-context-authority",
            harness_contract::context::ContextPressureState::new("default", 40_000, 7_500),
        )
        .with_ledger(harness_contract::context::ContextLedgerProjection {
            max_tokens: 40_000,
            consumed_tokens: 7_500,
            remaining_tokens: 32_500,
            tool_result_limit: 0,
            tool_result_consumed: 0,
            components: Vec::new(),
            request_sequence: 1,
            calibrated_input_tokens: Some(7_200),
        });

        store.complete(
            execution_id,
            &report,
            &[],
            "terminal-context-authority".to_string(),
        );

        let usage = store
            .execution_live(execution_id)
            .and_then(|live| live.context_usage)
            .expect("terminal context usage");
        assert_eq!(usage.model.as_deref(), Some("provider-model"));
        assert_eq!(usage.window_tokens, Some(128_000));
        assert_eq!(usage.window_source.as_deref(), Some("registry"));
        assert_eq!(usage.input_tokens, Some(7_200));
        assert_eq!(usage.remaining_tokens, Some(120_800));
        assert_eq!(usage.usage_percent_bp, Some(562));
        assert_eq!(usage.input_source.as_deref(), Some("provider_actual"));
    }

    #[test]
    fn execution_graph_identity_is_persisted_separately_from_ingress_identity() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        let execution_id = "session-ingress-graph:identity";
        let session_id = "session-graph-identity";
        let context = CowdExecutionContext {
            execution_id: execution_id.to_string(),
            session_id: session_id.to_string(),
            turn_id: "turn-graph-identity".to_string(),
        };
        store.record_queued(
            session_id,
            execution_id.to_string(),
            context.turn_id.clone(),
        );
        store.observe_event(
            session_id,
            &CowdEvent::ExecutionScoped {
                context,
                activity_binding: None,
                event: Box::new(CowdEvent::ExecutionGraphSummary {
                    summary: crate::RuntimeExecutionGraphSummary {
                        graph_id: Some("execution-graph:queryable".to_string()),
                        board_id: None,
                        status: "running".to_string(),
                        agent_tasks: 1,
                        child_executions: 0,
                        memory_candidates: 0,
                        conflicts: 0,
                        completion_rate: Some(0.0),
                        synthesis_lift: None,
                        complementarity_score: None,
                    },
                }),
            },
        );

        let index = store.session_execution_index(session_id);
        assert_eq!(index.latest_execution_id.as_deref(), Some(execution_id));
        assert_eq!(
            index.latest_graph_id.as_deref(),
            Some("execution-graph:queryable")
        );
        let recovered = ExecutionLiveStore::new(event_store).session_execution_index(session_id);
        assert_eq!(
            recovered.latest_graph_id.as_deref(),
            Some("execution-graph:queryable")
        );
    }

    #[test]
    fn blocked_terminal_remains_error_after_ledger_rehydration() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        let execution_id = "execution-blocked";
        store.record_queued(
            "session-blocked",
            execution_id.to_string(),
            "turn-blocked".to_string(),
        );
        let report = ContextTurnReport::new(
            "turn-blocked",
            harness_contract::context::ContextPressureState::new("default", 32_000, 100),
        );
        store.block(
            execution_id,
            &report,
            &["src/lib.rs".to_string()],
            "terminal-blocked".to_string(),
            "provider protocol remained invalid".to_string(),
        );

        let assert_blocked = |live: ExecutionLiveState| {
            assert_eq!(live.status, ExecutionLiveStatus::Error);
            assert_eq!(live.terminal_ref.as_deref(), Some("terminal-blocked"));
            assert_eq!(
                live.error.as_deref(),
                Some("provider protocol remained invalid")
            );
            assert_eq!(live.metrics.files_touched, 1);
        };
        assert_blocked(store.execution_live(execution_id).unwrap());
        let rehydrated = ExecutionLiveStore::new(Arc::clone(&event_store));
        assert_blocked(rehydrated.execution_live(execution_id).unwrap());
        rehydrated.release_terminal_checkpoint(execution_id);
        assert!(event_store
            .projection_checkpoint(&live_projection_id(execution_id))
            .unwrap()
            .is_none());
    }

    #[test]
    fn completion_preserves_the_provider_observed_effective_model() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(event_store);
        store.record_queued(
            "session-model",
            "execution-model".to_string(),
            "turn-model".to_string(),
        );
        store.observe_event(
            "session-model",
            &CowdEvent::ExecutionScoped {
                context: CowdExecutionContext {
                    execution_id: "execution-model".to_string(),
                    session_id: "session-model".to_string(),
                    turn_id: "turn-model".to_string(),
                },
                activity_binding: None,
                event: Box::new(CowdEvent::RunModelTelemetry {
                    telemetry: crate::RunModelTelemetry {
                        model: Some("effective-provider-model".to_string()),
                        models_used: vec!["effective-provider-model".to_string()],
                        first_token_latency_ms: Some(10),
                        active_stream_duration_ms: Some(20),
                        wall_duration_ms: 30,
                        output_chars: 40,
                        output_chunks: 2,
                        input_tokens: 12,
                        output_tokens: 3,
                        cache_create_tokens: 0,
                        cache_read_tokens: 0,
                        cache_dimensions_known: true,
                        total_tokens: 15,
                        usage_source: "provider_actual".to_string(),
                        wall_chars_per_second: None,
                        wall_tokens_per_second: None,
                        active_chars_per_second: None,
                        active_tokens_per_second: None,
                        chars_per_second: None,
                        tokens_per_second: None,
                    },
                }),
            },
        );
        let report = ContextTurnReport::new(
            "turn-model",
            harness_contract::context::ContextPressureState::new("default", 32_000, 12),
        )
        .with_ledger(harness_contract::context::ContextLedgerProjection {
            max_tokens: 32_000,
            consumed_tokens: 12,
            remaining_tokens: 31_988,
            tool_result_limit: 0,
            tool_result_consumed: 0,
            components: Vec::new(),
            request_sequence: 1,
            calibrated_input_tokens: Some(12),
        });

        store.complete(
            "execution-model",
            &report,
            &[],
            "terminal-model".to_string(),
        );

        assert_eq!(
            store
                .execution_live("execution-model")
                .and_then(|live| live.context_usage)
                .and_then(|usage| usage.model),
            Some("effective-provider-model".to_string())
        );
    }

    #[test]
    fn terminal_claim_recovers_from_a_stale_cached_row_revision() {
        let event_store = Arc::new(RuntimeEventStore::for_test());
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        let execution_id = "execution-stale-claim";
        store.record_queued(
            "session-stale",
            execution_id.to_string(),
            "turn-stale".to_string(),
        );
        // Simulate a concurrent writer advancing the durable projection row
        // after this store cached its revision.
        let projection_id = live_projection_id(execution_id);
        let checkpoint = event_store
            .projection_checkpoint(&projection_id)
            .unwrap()
            .expect("queued record persisted a checkpoint");
        event_store
            .compare_and_put_projection_checkpoint(
                &projection_id,
                checkpoint.source_cursor,
                checkpoint.revision,
                &checkpoint.payload,
                current_time_ms(),
            )
            .expect("concurrent advance");
        let claim = store
            .claim_terminal(
                execution_id,
                "stale-claim".to_string(),
                ExecutionLiveStatus::Cancelled,
                1,
            )
            .expect("terminal claim retries a stale cached revision");
        assert!(matches!(claim, TerminalFenceClaim::Claimed));
    }
}
