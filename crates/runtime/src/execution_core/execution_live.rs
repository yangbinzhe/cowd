//! Runtime-owned, execution-correlated live lifecycle projection.
//!
//! The durable event ledger stores immutable snapshots while this small cache
//! serves hot reads. Gateway may call the public RuntimeServices facade but
//! never owns transitions, revisions, metrics, or terminal state.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use harness_contract::context::ContextTurnReport;
use harness_contract::projection::{
    ContextUsageProjection, ExecutionLiveState, ExecutionLiveStatus, RunMetricsProjection,
    SessionExecutionIndexProjection,
};
use serde::{Deserialize, Serialize};

use crate::{CowdEvent, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore};

const LIVE_EVENT_KIND: &str = "execution.live.snapshot.v1";
const LIVE_OUTPUT_PREVIEW_LIMIT: usize = 1_024;
const LIVE_EVENT_SCAN_LIMIT: usize = 10_000;
const LIVE_CACHE_MAX_RECORDS: usize = 512;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveExecutionRecord {
    session_id: String,
    execution_id: String,
    live: ExecutionLiveState,
    #[serde(default)]
    tool_ids: BTreeSet<String>,
    #[serde(default)]
    approval_keys: BTreeSet<String>,
}

impl LiveExecutionRecord {
    fn new(session_id: String, execution_id: String, turn_id: String) -> Self {
        let now = current_time_ms();
        Self {
            session_id,
            execution_id,
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
                output_preview: None,
                terminal_ref: None,
                error: None,
            },
            tool_ids: BTreeSet::new(),
            approval_keys: BTreeSet::new(),
        }
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
        true
    }

    fn touch(&mut self) {
        let now = current_time_ms();
        self.live.updated_at_ms = now;
        self.live.last_progress_at_ms = now;
        self.live.revision = self.live.revision.saturating_add(1);
    }

    fn append_preview(&mut self, text: &str) {
        let mut preview = self.live.output_preview.take().unwrap_or_default();
        preview.push_str(text);
        if preview.len() > LIVE_OUTPUT_PREVIEW_LIMIT {
            let split = preview.len().saturating_sub(LIVE_OUTPUT_PREVIEW_LIMIT);
            let boundary = preview
                .char_indices()
                .find_map(|(index, _)| (index >= split).then_some(index))
                .unwrap_or(0);
            preview = preview[boundary..].to_string();
        }
        self.live.output_preview = (!preview.is_empty()).then_some(preview);
        self.touch();
    }

    fn complete(&mut self, report: &ContextTurnReport, terminal_ref: String) -> bool {
        self.live.context_usage = context_usage_from_report(report);
        self.live.metrics.tool_calls = self
            .live
            .metrics
            .tool_calls
            .max(report.observations.len() as u64);
        self.live.metrics.memory_evidence = self
            .live
            .metrics
            .memory_evidence
            .max(report.audit_projections.len() as u64);
        self.live.metrics.context_items = self.live.context_usage.as_ref().map_or(0, |usage| {
            usage
                .components
                .iter()
                .map(|component| component.occurrences)
                .sum()
        });
        self.live.terminal_ref = Some(terminal_ref);
        self.live.error = None;
        self.transition(
            ExecutionLiveStatus::Complete,
            Some("terminal committed".to_string()),
        )
    }

    fn fail(&mut self, error: String) -> bool {
        self.live.error = Some(error.clone());
        self.transition(ExecutionLiveStatus::Error, Some(error))
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
                record.append_preview(text);
                true
            }
            CowdEvent::ToolStart { id, .. } => {
                if record.tool_ids.insert(id.clone()) {
                    record.live.metrics.tool_calls =
                        record.live.metrics.tool_calls.saturating_add(1);
                    record.touch();
                    true
                } else {
                    false
                }
            }
            CowdEvent::ToolProgress { .. } | CowdEvent::ToolComplete { .. } => {
                record.touch();
                true
            }
            CowdEvent::ApprovalRequested { tool } => {
                if record.approval_keys.insert(tool.clone()) {
                    record.live.metrics.approvals = record.live.metrics.approvals.saturating_add(1);
                    record.touch();
                    true
                } else {
                    false
                }
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
            CowdEvent::RunModelTelemetry { telemetry } => {
                record.live.metrics.input_tokens = telemetry.input_tokens;
                record.live.metrics.output_tokens = telemetry.output_tokens;
                record.live.metrics.total_tokens = telemetry.total_tokens;
                let mut usage = record.live.context_usage.clone().unwrap_or_default();
                usage.model = telemetry.model.clone();
                usage.input_tokens = Some(telemetry.input_tokens);
                usage.input_source = Some(telemetry.usage_source.clone());
                update_usage_percent(&mut usage);
                record.live.context_usage = Some(usage);
                record.touch();
                true
            }
            CowdEvent::TurnError { error } => record.fail(error.clone()),
            _ => false,
        };
        if changed {
            self.persist(record);
        }
        prune_terminal_cache(&mut records);
    }

    pub(crate) fn complete(
        &self,
        execution_id: &str,
        report: &ContextTurnReport,
        terminal_ref: String,
    ) {
        self.update_record(execution_id, |record| record.complete(report, terminal_ref));
    }

    pub(crate) fn fail(&self, execution_id: &str, error: String) {
        self.update_record(execution_id, |record| record.fail(error));
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
        records.sort_by_key(|record| record.live.updated_at_ms);
        let latest = records.last();
        SessionExecutionIndexProjection {
            session_id: session_id.to_string(),
            active_execution_ids: records
                .iter()
                .filter(|record| !record.live.status.is_terminal())
                .map(|record| record.execution_id.clone())
                .collect(),
            latest_execution_id: latest.map(|record| record.execution_id.clone()),
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
        self.event_store
            .list_stream(execution_id)
            .ok()?
            .into_iter()
            .rev()
            .find(|event| event.kind == LIVE_EVENT_KIND)
            .and_then(|event| serde_json::from_value(event.payload).ok())
    }

    fn persist(&self, record: &LiveExecutionRecord) {
        if let Err(error) = self.event_store.append(RuntimeEventInput {
            stream_id: record.execution_id.clone(),
            scope: RuntimeEventScope::ExecutionGraph,
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

        let rehydrated = ExecutionLiveStore::new(event_store);
        assert_eq!(
            rehydrated.execution_live("execution-a").unwrap().status,
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
}
