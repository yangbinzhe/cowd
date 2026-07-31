//! Runtime-owned, execution-correlated live lifecycle projection.
//!
//! The durable event ledger stores immutable snapshots while this small cache
//! serves hot reads. Gateway may call the public RuntimeServices facade but
//! never owns transitions, revisions, metrics, or terminal state.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use harness_contract::context::ContextTurnReport;
use harness_contract::projection::{
    ContextUsageProjection, ExecutionLatencyProjection, ExecutionLiveOutputPart,
    ExecutionLiveState, ExecutionLiveStatus, RunMetricsProjection, SessionExecutionEntryProjection,
    SessionExecutionIndexProjection,
};
use serde::{Deserialize, Serialize};

use crate::{CowdEvent, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore};

const LIVE_EVENT_KIND: &str = "execution.live.snapshot.v1";
// Keep enough canonical live text to repair a saturated Surface stream while
// remaining bounded per active execution. The byte offset carried alongside
// the snapshot makes truncation explicit instead of silently presenting a
// suffix as a complete answer.
const LIVE_OUTPUT_PREVIEW_LIMIT: usize = 1024 * 1024;
const LIVE_EVENT_SCAN_LIMIT: usize = 10_000;
const LIVE_CACHE_MAX_RECORDS: usize = 512;

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

/// Live execution snapshots are an execution-correlated projection, not
/// `ExecutionGraph` events.  Keeping a separate stream prevents early status
/// updates (queued/preparing/model) from consuming the canonical graph's
/// revision zero before graph registration.
fn live_stream_id(execution_id: &str) -> String {
    format!("execution-live:{execution_id}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveExecutionRecord {
    session_id: String,
    execution_id: String,
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
    own_model_usage: Option<crate::RunModelTelemetry>,
    #[serde(default)]
    descendant_model_usage: BTreeMap<String, crate::RunModelTelemetry>,
}

impl LiveExecutionRecord {
    fn new(session_id: String, execution_id: String, turn_id: String) -> Self {
        let now = current_time_ms();
        Self {
            session_id,
            execution_id,
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
            own_model_usage: None,
            descendant_model_usage: BTreeMap::new(),
        }
    }

    fn refresh_model_usage_metrics(&mut self) {
        let mut input_tokens = 0_u64;
        let mut output_tokens = 0_u64;
        let mut total_tokens = 0_u64;
        for telemetry in self
            .own_model_usage
            .iter()
            .chain(self.descendant_model_usage.values())
        {
            input_tokens = input_tokens.saturating_add(telemetry.input_tokens);
            output_tokens = output_tokens.saturating_add(telemetry.output_tokens);
            total_tokens = total_tokens.saturating_add(telemetry.total_tokens);
        }
        self.live.metrics.input_tokens = input_tokens;
        self.live.metrics.output_tokens = output_tokens;
        self.live.metrics.total_tokens = total_tokens;
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
            tracing::warn!(
                execution_id = %self.execution_id,
                from = ?self.live.status,
                to = ?status,
                "ignored invalid Runtime live execution status transition"
            );
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
            self.live
                .output_parts
                .last_mut()
                .expect("new output part exists")
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
        self.apply_terminal_projection(report, write_attempt_paths, terminal_ref);
        self.live.error = None;
        self.transition(
            ExecutionLiveStatus::Complete,
            Some("terminal committed".to_string()),
        )
    }

    fn complete_recovered(&mut self, terminal_ref: String) -> bool {
        self.live.terminal_ref = Some(terminal_ref);
        self.live.error = None;
        self.transition(
            ExecutionLiveStatus::Complete,
            Some("durable terminal recovered".to_string()),
        )
    }

    fn fail(&mut self, error: String) -> bool {
        self.live.error = Some(error.clone());
        self.transition(ExecutionLiveStatus::Error, Some(error))
    }

    fn block(
        &mut self,
        report: &ContextTurnReport,
        write_attempt_paths: &[String],
        terminal_ref: String,
        reason: String,
    ) -> bool {
        self.apply_terminal_projection(report, write_attempt_paths, terminal_ref);
        self.live.error = Some(reason.clone());
        self.transition(ExecutionLiveStatus::Error, Some(reason))
    }

    fn cancel(&mut self, detail: String) -> bool {
        self.live.error = None;
        self.transition(ExecutionLiveStatus::Cancelled, Some(detail))
    }
}

/// The sole lifecycle reducer for provider-backed session executions.
pub(crate) struct ExecutionLiveStore {
    event_store: Arc<RuntimeEventStore>,
    records: Mutex<BTreeMap<String, LiveExecutionRecord>>,
}

impl ExecutionLiveStore {
    pub(crate) fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        Self {
            event_store,
            records: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn record_queued(&self, session_id: &str, execution_id: String, turn_id: String) {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if records.contains_key(&execution_id) {
            return;
        }
        let record = self.load_record(&execution_id).unwrap_or_else(|| {
            LiveExecutionRecord::new(session_id.to_string(), execution_id.clone(), turn_id)
        });
        self.persist(&record);
        records.insert(execution_id, record);
        prune_terminal_cache(&mut records);
    }

    pub(crate) fn observe_event(&self, expected_session_id: &str, event: &CowdEvent) {
        let Some(context) = event.execution_context() else {
            return;
        };
        let parent_execution_id = event
            .execution_lineage()
            .map(|lineage| lineage.parent_execution_id.clone());
        if context.session_id != expected_session_id {
            tracing::warn!(
                expected_session_id,
                event_session_id = %context.session_id,
                execution_id = %context.execution_id,
                "ignored execution event delivered through the wrong session relay"
            );
            return;
        }
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = records
            .entry(context.execution_id.clone())
            .or_insert_with(|| {
                self.load_record(&context.execution_id).unwrap_or_else(|| {
                    LiveExecutionRecord::new(
                        context.session_id.clone(),
                        context.execution_id.clone(),
                        context.turn_id.clone(),
                    )
                })
            });
        let changed = match event.domain_event() {
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
        let checkpoint_record = checkpoint.then(|| record.clone());
        prune_terminal_cache(&mut records);
        drop(records);
        if let Some(record) = checkpoint_record.as_ref() {
            self.persist(record);
        }
        if let Some(parent_execution_id) = parent_execution_id {
            self.observe_descendant_event(&parent_execution_id, context, event);
        }
    }

    fn observe_descendant_event(
        &self,
        parent_execution_id: &str,
        child_context: &crate::CowdExecutionContext,
        event: &CowdEvent,
    ) {
        let mut records = self
            .records
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
                        changed |= record.memory_item_ids.insert(item_id);
                        for evidence_id in &item.evidence {
                            changed |= record
                                .memory_evidence_ids
                                .insert(format!("{child_prefix}{evidence_id}"));
                        }
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
        drop(records);
        if let Some(record) = checkpoint_record.as_ref() {
            self.persist(record);
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

    pub(crate) fn complete_recovered(&self, execution_id: &str, terminal_ref: String) {
        self.update_record(execution_id, |record| {
            record.complete_recovered(terminal_ref)
        });
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

    pub(crate) fn cancel(&self, execution_id: &str, detail: String) {
        self.update_record(execution_id, |record| record.cancel(detail));
    }

    pub(crate) fn execution_live(&self, execution_id: &str) -> Option<ExecutionLiveState> {
        if let Some(record) = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(execution_id)
            .cloned()
        {
            return Some(record.live);
        }
        let record = self.load_record(execution_id)?;
        let live = record.live.clone();
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(execution_id.to_string(), record);
        Some(live)
    }

    pub(crate) fn session_execution_index(
        &self,
        session_id: &str,
    ) -> SessionExecutionIndexProjection {
        let mut records = self.records_for_session(session_id);
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
        SessionExecutionIndexProjection {
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
        }
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
    ) {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = records.get_mut(execution_id) else {
            tracing::warn!(
                execution_id,
                "Runtime live update ignored because execution was never registered"
            );
            return;
        };
        if update(record) {
            self.persist(record);
        }
        prune_terminal_cache(&mut records);
    }

    fn records_for_session(&self, session_id: &str) -> Vec<LiveExecutionRecord> {
        self.all_records()
            .into_iter()
            .filter(|record| record.session_id == session_id)
            .collect()
    }

    fn all_records(&self) -> Vec<LiveExecutionRecord> {
        let mut merged = self
            .event_store
            .all_events(LIVE_EVENT_SCAN_LIMIT)
            .unwrap_or_default()
            .into_iter()
            .filter(|event| event.kind == LIVE_EVENT_KIND)
            .collect::<Vec<_>>();
        merged.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
        let mut records = BTreeMap::new();
        for event in merged {
            if let Ok(record) = serde_json::from_value::<LiveExecutionRecord>(event.payload) {
                records.insert(record.execution_id.clone(), record);
            }
        }
        for record in self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
        {
            records.insert(record.execution_id.clone(), record);
        }
        records.into_values().collect()
    }

    fn load_record(&self, execution_id: &str) -> Option<LiveExecutionRecord> {
        let decode_latest = |stream_id: &str| {
            self.event_store
                .list_stream(stream_id)
                .ok()
                .and_then(|events| {
                    events
                        .into_iter()
                        .rev()
                        .find(|event| event.kind == LIVE_EVENT_KIND)
                        .and_then(|event| serde_json::from_value(event.payload).ok())
                })
        };
        // V504 stored snapshots on the graph stream.  Keep a read-only
        // fallback so an in-flight execution survives the V505 stream split;
        // all V505+ writes use the dedicated stream above.
        decode_latest(&live_stream_id(execution_id)).or_else(|| decode_latest(execution_id))
    }

    fn persist(&self, record: &LiveExecutionRecord) {
        if let Err(error) = self.event_store.append(RuntimeEventInput {
            stream_id: live_stream_id(&record.execution_id),
            scope: RuntimeEventScope::ExecutionLive,
            kind: LIVE_EVENT_KIND.to_string(),
            status: Some(format!("{:?}", record.live.status).to_lowercase()),
            actor: Some("runtime_live_reducer".to_string()),
            refs: vec![
                RuntimeEventRef {
                    kind: "execution_graph".to_string(),
                    id: record.execution_id.clone(),
                },
                RuntimeEventRef {
                    kind: "session".to_string(),
                    id: record.session_id.clone(),
                },
                RuntimeEventRef {
                    kind: "turn".to_string(),
                    id: record.live.turn_id.clone().unwrap_or_default(),
                },
            ],
            payload: serde_json::to_value(record).unwrap_or_else(|serialization_error| {
                serde_json::json!({ "serialization_error": serialization_error.to_string() })
            }),
        }) {
            tracing::error!(
                execution_id = %record.execution_id,
                error = %error,
                "failed to persist Runtime live execution snapshot"
            );
        }
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

/// Keep the hot projection bounded without ever evicting an active execution.
/// Historical/terminal reads remain recoverable from the Runtime event ledger.
fn prune_terminal_cache(records: &mut BTreeMap<String, LiveExecutionRecord>) {
    let overflow = records.len().saturating_sub(LIVE_CACHE_MAX_RECORDS);
    if overflow == 0 {
        return;
    }
    let removable = records
        .iter()
        .filter(|(_, record)| record.live.status.is_terminal())
        .map(|(execution_id, record)| (record.live.updated_at_ms, execution_id.clone()))
        .collect::<Vec<_>>();
    for (_, execution_id) in removable.into_iter().take(overflow) {
        records.remove(&execution_id);
    }
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
            CallingModel | CallingTool | Thinking | Finalizing | Error | Cancelled
        ) | (Finalizing, Complete | Error | Cancelled)
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
    use crate::{CowdExecutionContext, RuntimeEventStore};

    #[test]
    fn completed_tool_plan_is_visible_before_execution_and_counted_once() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let store = ExecutionLiveStore::new(event_store);
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
                event: Box::new(CowdEvent::ToolStart {
                    id: "search-call".to_string(),
                    name: "web_search".to_string(),
                    preview: r#"{"query":"WAIC"}"#.to_string(),
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
    }

    #[test]
    fn scoped_event_updates_only_its_execution_and_rehydrates_from_runtime_ledger() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
        assert_eq!(event_store.stream_revision("execution-a").unwrap(), 0);
        assert_eq!(
            event_store
                .stream_revision(&live_stream_id("execution-a"))
                .unwrap(),
            2
        );
        assert!(event_store
            .list_scope(RuntimeEventScope::ExecutionGraph, 10)
            .unwrap()
            .is_empty());
        assert_eq!(
            event_store
                .list_scope(RuntimeEventScope::ExecutionLive, 10)
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn live_output_recovery_uses_runtime_projection_identity_across_text_items() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
    fn durable_terminal_recovery_closes_a_finalizing_live_projection_idempotently() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let store = ExecutionLiveStore::new(Arc::clone(&event_store));
        let execution_id = "execution-recovered-terminal";
        store.record_queued(
            "session-recovered-terminal",
            execution_id.to_string(),
            "turn-recovered-terminal".to_string(),
        );
        let phase = |status, detail: &str| CowdEvent::ExecutionScoped {
            context: CowdExecutionContext {
                execution_id: execution_id.to_string(),
                session_id: "session-recovered-terminal".to_string(),
                turn_id: "turn-recovered-terminal".to_string(),
            },
            event: Box::new(CowdEvent::ExecutionPhase {
                status,
                detail: Some(detail.to_string()),
            }),
        };
        store.observe_event(
            "session-recovered-terminal",
            &phase(ExecutionLiveStatus::CallingModel, "calling model"),
        );
        store.observe_event(
            "session-recovered-terminal",
            &phase(ExecutionLiveStatus::Finalizing, "synthesizing terminal"),
        );

        store.complete_recovered(execution_id, "terminal-recovered".to_string());
        let terminal = store.execution_live(execution_id).unwrap();
        assert_eq!(terminal.status, ExecutionLiveStatus::Complete);
        assert_eq!(
            terminal.status_detail.as_deref(),
            Some("durable terminal recovered")
        );
        assert_eq!(terminal.terminal_ref.as_deref(), Some("terminal-recovered"));
        let terminal_revision = terminal.revision;

        store.complete_recovered(execution_id, "terminal-recovered".to_string());
        assert_eq!(
            store.execution_live(execution_id).unwrap().revision,
            terminal_revision,
            "replaying the same durable terminal must be idempotent"
        );

        let rehydrated = ExecutionLiveStore::new(event_store);
        assert_eq!(
            rehydrated.execution_live(execution_id).unwrap().status,
            ExecutionLiveStatus::Complete,
            "the repaired terminal projection must survive process restart"
        );
    }

    #[test]
    fn approval_metrics_deduplicate_stable_request_ids_across_replay_and_restart() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
    fn legacy_graph_stream_snapshot_rehydrates_after_live_stream_split() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let mut legacy_record = LiveExecutionRecord::new(
            "session-legacy".to_string(),
            "execution-legacy".to_string(),
            "turn-legacy".to_string(),
        );
        assert!(legacy_record.transition(
            ExecutionLiveStatus::CallingModel,
            Some("legacy provider request".to_string()),
        ));
        event_store
            .append(RuntimeEventInput {
                stream_id: legacy_record.execution_id.clone(),
                scope: RuntimeEventScope::ExecutionGraph,
                kind: LIVE_EVENT_KIND.to_string(),
                status: Some("calling_model".to_string()),
                actor: Some("runtime_live_reducer".to_string()),
                refs: Vec::new(),
                payload: serde_json::to_value(&legacy_record).unwrap(),
            })
            .unwrap();

        let rehydrated = ExecutionLiveStore::new(event_store);
        assert_eq!(
            rehydrated
                .execution_live("execution-legacy")
                .unwrap()
                .status,
            ExecutionLiveStatus::CallingModel
        );
    }

    #[test]
    fn hot_cache_prunes_only_terminal_records() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let store = ExecutionLiveStore::new(event_store);
        for index in 0..=LIVE_CACHE_MAX_RECORDS {
            let execution_id = format!("terminal-{index}");
            store.record_queued("session-a", execution_id.clone(), format!("turn-{index}"));
            store.cancel(&execution_id, "complete for cache test".to_string());
        }
        store.record_queued(
            "session-a",
            "active-execution".to_string(),
            "active-turn".to_string(),
        );

        let records = store
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(records.len() <= LIVE_CACHE_MAX_RECORDS);
        assert!(records.contains_key("active-execution"));
    }

    #[test]
    fn completion_projects_real_memory_recall_occurrences_from_the_context_ledger() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
        assert_blocked(
            ExecutionLiveStore::new(event_store)
                .execution_live(execution_id)
                .unwrap(),
        );
    }

    #[test]
    fn completion_preserves_the_provider_observed_effective_model() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
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
}
