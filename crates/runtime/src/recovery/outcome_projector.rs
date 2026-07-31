//! Recoverable, bounded read projection for canonical execution outcomes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use harness_contract::outcome::{ExecutionOutcome, OutcomeSegmentKey, OUTCOME_SCHEMA_REVISION};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    CancellationToken, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    RuntimeTransactionEventInput,
};

use crate::execution_core::outcome_service::OUTCOME_EVENT_KIND;

const PROJECTOR_STREAM: &str = "outcome-projector";
const CHECKPOINT_KIND: &str = "runtime.outcome.projector.checkpoint.v1";
const DLQ_KIND: &str = "runtime.outcome.projector.failed.v1";
const PROJECTOR_BATCH: usize = 128;
const MAX_OBSERVATIONS_PER_SEGMENT: usize = 1_024;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeProjectionCheckpoint {
    pub source_cursor: u64,
    pub snapshot_revision: u64,
    pub projected_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeProjectionDlqEntry {
    pub source_event_id: String,
    pub source_cursor: u64,
    pub error: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeSegmentSnapshot {
    pub key: Option<OutcomeSegmentKey>,
    pub sample_count: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub evidence_complete_count: u64,
    pub paired_sample_count: u64,
    pub quality_observed_count: u64,
    pub quality_mean_bp: Option<u16>,
    pub last_observed_at_ms: u64,
    pub duration_p50_ms: u64,
    pub duration_p95_ms: u64,
    pub total_tokens_p50: u64,
    pub total_tokens_p95: u64,
    pub terminal_class_counts: BTreeMap<String, u64>,
    pub schema_revisions: Vec<u32>,
    #[serde(default)]
    pub(crate) observations: Vec<OutcomeObservationSample>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OutcomeObservationSample {
    outcome_id: String,
    duration_ms: u64,
    total_tokens: u64,
    succeeded: bool,
    terminal_class: String,
    paired_sample_id: Option<String>,
    schema_revision: u32,
    quality_observed: bool,
    quality_bp: Option<u16>,
    evidence_complete: bool,
    observed_at_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeReadSnapshot {
    pub revision: u64,
    pub source_cursor: u64,
    pub projected_at_ms: u64,
    pub segments: BTreeMap<String, OutcomeSegmentSnapshot>,
    pub dlq_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeProjectionHealth {
    pub checkpoint_cursor: u64,
    pub latest_commit_cursor: u64,
    pub lag_commits: u64,
    pub projected_at_ms: u64,
    pub freshness_ms: u64,
    pub dlq_count: u64,
    pub worker_running: bool,
}

impl OutcomeReadSnapshot {
    #[must_use]
    pub fn hash(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        format!("{:x}", Sha256::digest(bytes))
    }
}

pub struct OutcomeProjector {
    event_store: Arc<RuntimeEventStore>,
    snapshot: RwLock<Arc<OutcomeReadSnapshot>>,
    projection_lock: Mutex<()>,
    cancellation: CancellationToken,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl OutcomeProjector {
    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        let snapshot = restore_and_replay(&event_store).unwrap_or_default();
        Self {
            event_store,
            snapshot: RwLock::new(Arc::new(snapshot)),
            projection_lock: Mutex::new(()),
            cancellation: CancellationToken::new(),
            worker: Mutex::new(None),
        }
    }

    pub fn start(self: &Arc<Self>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            return;
        }
        let projector = Arc::clone(self);
        *worker = Some(handle.spawn(async move {
            let mut commits = projector.event_store.subscribe_commits();
            loop {
                if let Err(error) = projector.project_available(PROJECTOR_BATCH) {
                    tracing::warn!(%error, "outcome projector pass failed");
                }
                tokio::select! {
                    _ = projector.cancellation.cancelled() => break,
                    changed = commits.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                }
            }
        }));
    }

    pub async fn shutdown(&self) {
        self.cancellation.cancel();
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            let _ = worker.await;
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<OutcomeReadSnapshot> {
        Arc::clone(
            &self
                .snapshot
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    pub fn project_available(&self, max_commits: usize) -> Result<usize, String> {
        let _projection_guard = self
            .projection_lock
            .lock()
            .map_err(|_| "outcome projection lock poisoned".to_string())?;
        let current = self.snapshot();
        let mut next = (*current).clone();
        let batches = self
            .event_store
            .events_after_cursor(current.source_cursor, max_commits.max(1))
            .map_err(|error| error.to_string())?;
        if batches.is_empty() {
            return Ok(0);
        }
        let mut processed = 0;
        let mut source_changed = false;
        for batch in batches {
            for event in &batch.events {
                if event.stream_id == PROJECTOR_STREAM {
                    continue;
                }
                if event.kind != OUTCOME_EVENT_KIND {
                    continue;
                }
                source_changed = true;
                match decode_outcome(event.payload.clone()) {
                    Ok(outcome) => reduce_outcome(&mut next, outcome),
                    Err(error) => {
                        self.dead_letter(&event.event_id, batch.commit_cursor, &error.to_string())?;
                        next.dlq_count = next.dlq_count.saturating_add(1);
                    }
                }
            }
            next.source_cursor = batch.commit_cursor;
            processed += 1;
        }
        if !source_changed {
            *self
                .snapshot
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(next);
            return Ok(processed);
        }
        next.revision = next.revision.saturating_add(1);
        next.projected_at_ms = latest_observed_at(&next);
        self.checkpoint(&next)?;
        *self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(next);
        Ok(processed)
    }

    pub fn health(&self) -> Result<OutcomeProjectionHealth, String> {
        let snapshot = self.snapshot();
        let (latest_commit_cursor, lag_commits) =
            outcome_source_lag(&self.event_store, snapshot.source_cursor)?;
        Ok(OutcomeProjectionHealth {
            checkpoint_cursor: snapshot.source_cursor,
            latest_commit_cursor,
            lag_commits,
            projected_at_ms: snapshot.projected_at_ms,
            freshness_ms: now_ms().saturating_sub(snapshot.projected_at_ms),
            dlq_count: snapshot.dlq_count,
            worker_running: self
                .worker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_some_and(|worker| !worker.is_finished()),
        })
    }

    pub fn replay(&self) -> Result<OutcomeReadSnapshot, String> {
        let mut replay = OutcomeReadSnapshot::default();
        let mut cursor = 0;
        loop {
            let batches = self
                .event_store
                .events_after_cursor(cursor, PROJECTOR_BATCH)
                .map_err(|error| error.to_string())?;
            if batches.is_empty() {
                break;
            }
            for batch in batches {
                for event in &batch.events {
                    if event.kind == OUTCOME_EVENT_KIND {
                        match decode_outcome(event.payload.clone()) {
                            Ok(outcome) => reduce_outcome(&mut replay, outcome),
                            Err(error) => {
                                record_outcome_dlq(
                                    &self.event_store,
                                    &event.event_id,
                                    batch.commit_cursor,
                                    &error,
                                )?;
                            }
                        }
                    }
                }
                cursor = batch.commit_cursor;
            }
        }
        replay.source_cursor = cursor;
        replay.projected_at_ms = self.snapshot().projected_at_ms;
        replay.revision = self.snapshot().revision;
        replay.dlq_count = self.snapshot().dlq_count;
        Ok(replay)
    }

    fn checkpoint(&self, snapshot: &OutcomeReadSnapshot) -> Result<(), String> {
        let source_cursor = snapshot.source_cursor;
        let key = format!("source-cursor:{source_cursor}");
        if self
            .event_store
            .event_by_idempotency_key(PROJECTOR_STREAM, &key)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Ok(());
        }
        let revision = self
            .event_store
            .stream_revision(PROJECTOR_STREAM)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                PROJECTOR_STREAM,
                revision,
                format!("outcome-projector:{source_cursor}"),
                vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: PROJECTOR_STREAM.to_string(),
                        scope: RuntimeEventScope::Recovery,
                        kind: CHECKPOINT_KIND.to_string(),
                        status: Some("completed".to_string()),
                        actor: Some("runtime.outcome_projector".to_string()),
                        refs: Vec::new(),
                        payload: serde_json::json!({
                            "checkpoint": OutcomeProjectionCheckpoint {
                                source_cursor,
                                snapshot_revision: snapshot.revision,
                                projected_at_ms: snapshot.projected_at_ms,
                            },
                            "snapshot": snapshot,
                            "snapshot_hash": snapshot.hash(),
                        }),
                    },
                    idempotency_key: Some(key),
                    schema_version: 1,
                }],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn dead_letter(&self, event_id: &str, source_cursor: u64, error: &str) -> Result<(), String> {
        record_outcome_dlq(&self.event_store, event_id, source_cursor, error)
    }
}

fn outcome_source_lag(
    event_store: &RuntimeEventStore,
    source_cursor: u64,
) -> Result<(u64, u64), String> {
    let upper_bound = *event_store.subscribe_commits().borrow();
    let mut cursor = source_cursor;
    let mut latest_source_cursor = source_cursor;
    let mut lag_commits = 0_u64;
    while cursor < upper_bound {
        let batches = event_store
            .events_after_cursor(cursor, PROJECTOR_BATCH)
            .map_err(|error| error.to_string())?;
        if batches.is_empty() {
            break;
        }
        for batch in batches {
            cursor = batch.commit_cursor;
            if batch
                .events
                .iter()
                .any(|event| event.kind == OUTCOME_EVENT_KIND)
            {
                latest_source_cursor = batch.commit_cursor;
                lag_commits = lag_commits.saturating_add(1);
            }
        }
    }
    Ok((latest_source_cursor, lag_commits))
}

fn restore_latest_snapshot(event_store: &RuntimeEventStore) -> Result<OutcomeReadSnapshot, String> {
    event_store
        .list_stream(PROJECTOR_STREAM)?
        .into_iter()
        .rev()
        .find(|event| event.kind == CHECKPOINT_KIND)
        .and_then(|event| event.payload.get("snapshot").cloned())
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| error.to_string())
        .map(Option::unwrap_or_default)
}

fn restore_and_replay(event_store: &RuntimeEventStore) -> Result<OutcomeReadSnapshot, String> {
    let mut snapshot = restore_latest_snapshot(event_store)?;
    let mut cursor = snapshot.source_cursor;
    let mut changed = false;
    loop {
        let batches = event_store
            .events_after_cursor(cursor, PROJECTOR_BATCH)
            .map_err(|error| error.to_string())?;
        if batches.is_empty() {
            break;
        }
        for batch in batches {
            for event in &batch.events {
                if event.kind == OUTCOME_EVENT_KIND {
                    match decode_outcome(event.payload.clone()) {
                        Ok(outcome) => {
                            reduce_outcome(&mut snapshot, outcome);
                            changed = true;
                        }
                        Err(error) => {
                            record_outcome_dlq(
                                event_store,
                                &event.event_id,
                                batch.commit_cursor,
                                &error,
                            )?;
                            snapshot.dlq_count = snapshot.dlq_count.saturating_add(1);
                        }
                    }
                }
            }
            cursor = batch.commit_cursor;
            snapshot.source_cursor = cursor;
        }
    }
    if changed {
        snapshot.revision = snapshot.revision.saturating_add(1);
        snapshot.projected_at_ms = latest_observed_at(&snapshot);
    }
    Ok(snapshot)
}

fn latest_observed_at(snapshot: &OutcomeReadSnapshot) -> u64 {
    snapshot
        .segments
        .values()
        .map(|segment| segment.last_observed_at_ms)
        .max()
        .unwrap_or_default()
}

fn record_outcome_dlq(
    event_store: &RuntimeEventStore,
    event_id: &str,
    source_cursor: u64,
    error: &str,
) -> Result<(), String> {
    let key = format!("dead-letter:{event_id}");
    if event_store
        .event_by_idempotency_key(PROJECTOR_STREAM, &key)
        .map_err(|store_error| store_error.to_string())?
        .is_some()
    {
        return Ok(());
    }
    event_store
        .append_batch_if_revision(
            PROJECTOR_STREAM,
            event_store
                .stream_revision(PROJECTOR_STREAM)
                .map_err(|store_error| store_error.to_string())?,
            format!("outcome-projector-dlq:{event_id}"),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: PROJECTOR_STREAM.to_string(),
                    scope: RuntimeEventScope::Recovery,
                    kind: DLQ_KIND.to_string(),
                    status: Some("blocked".to_string()),
                    actor: Some("runtime.outcome_projector".to_string()),
                    refs: vec![RuntimeEventRef {
                        kind: "source_event".to_string(),
                        id: event_id.to_string(),
                    }],
                    payload: serde_json::to_value(OutcomeProjectionDlqEntry {
                        source_event_id: event_id.to_string(),
                        source_cursor,
                        error: error.to_string(),
                    })
                    .map_err(|encode_error| encode_error.to_string())?,
                },
                idempotency_key: Some(key),
                schema_version: 1,
            }],
        )
        .map_err(|store_error| store_error.to_string())?;
    Ok(())
}

fn decode_outcome(payload: serde_json::Value) -> Result<ExecutionOutcome, String> {
    let outcome =
        serde_json::from_value::<ExecutionOutcome>(payload).map_err(|error| error.to_string())?;
    if outcome.schema_revision != OUTCOME_SCHEMA_REVISION {
        return Err(format!(
            "unsupported Outcome schema revision {}",
            outcome.schema_revision
        ));
    }
    Ok(outcome)
}

fn reduce_outcome(snapshot: &mut OutcomeReadSnapshot, outcome: ExecutionOutcome) {
    let key = OutcomeSegmentKey::from_outcome(&outcome);
    let key_id = serde_json::to_string(&key).unwrap_or_else(|_| "invalid-segment".to_string());
    let segment = snapshot
        .segments
        .entry(key_id)
        .or_insert_with(|| OutcomeSegmentSnapshot {
            key: Some(key),
            ..Default::default()
        });
    let outcome_id = format!(
        "{}:{}:{}",
        outcome.identity.execution_id,
        outcome.identity.terminal_generation,
        outcome.schema_revision
    );
    if segment
        .observations
        .iter()
        .any(|sample| sample.outcome_id == outcome_id)
    {
        return;
    }
    let total_tokens = [
        outcome.usage.input_tokens,
        outcome.usage.output_tokens,
        outcome.usage.cached_tokens,
    ]
    .into_iter()
    .flatten()
    .fold(0_u64, u64::saturating_add);
    segment.observations.push(OutcomeObservationSample {
        outcome_id,
        duration_ms: outcome.timing.duration_ms,
        total_tokens,
        succeeded: outcome.terminal.is_success(),
        terminal_class: outcome.terminal.class_name().to_string(),
        paired_sample_id: outcome.identity.paired_sample_id.clone(),
        schema_revision: outcome.schema_revision,
        quality_observed: !matches!(
            &outcome.quality,
            harness_contract::outcome::OutcomeQuality::Unknown
        ),
        quality_bp: match &outcome.quality {
            harness_contract::outcome::OutcomeQuality::Unknown => None,
            harness_contract::outcome::OutcomeQuality::Estimate { value_bp, .. } => Some(*value_bp),
        },
        evidence_complete: matches!(
            outcome.evidence_completeness,
            harness_contract::reality::EvidenceCompleteness::Sufficient
        ),
        observed_at_ms: outcome.observation.observed_at_ms,
    });
    if segment.observations.len() > MAX_OBSERVATIONS_PER_SEGMENT {
        segment.observations.remove(0);
    }
    recompute_segment(segment);
}

fn recompute_segment(segment: &mut OutcomeSegmentSnapshot) {
    segment.sample_count = segment.observations.len() as u64;
    segment.success_count = segment
        .observations
        .iter()
        .filter(|sample| sample.succeeded)
        .count() as u64;
    segment.failure_count = segment.sample_count.saturating_sub(segment.success_count);
    segment.quality_observed_count = segment
        .observations
        .iter()
        .filter(|sample| sample.quality_observed)
        .count() as u64;
    segment.paired_sample_count = segment
        .observations
        .iter()
        .filter(|sample| sample.paired_sample_id.is_some())
        .count() as u64;
    segment.evidence_complete_count = segment
        .observations
        .iter()
        .filter(|sample| sample.evidence_complete)
        .count() as u64;
    segment.terminal_class_counts.clear();
    for sample in &segment.observations {
        *segment
            .terminal_class_counts
            .entry(sample.terminal_class.clone())
            .or_default() += 1;
    }
    segment.schema_revisions = segment
        .observations
        .iter()
        .map(|sample| sample.schema_revision)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let quality_values = segment
        .observations
        .iter()
        .filter_map(|sample| sample.quality_bp)
        .collect::<Vec<_>>();
    segment.quality_mean_bp = (!quality_values.is_empty()).then(|| {
        let total = quality_values
            .iter()
            .fold(0_u64, |sum, value| sum.saturating_add(u64::from(*value)));
        u16::try_from(total / quality_values.len() as u64).unwrap_or(10_000)
    });
    segment.last_observed_at_ms = segment
        .observations
        .iter()
        .map(|sample| sample.observed_at_ms)
        .max()
        .unwrap_or_default();
    let mut durations = segment
        .observations
        .iter()
        .map(|sample| sample.duration_ms)
        .collect::<Vec<_>>();
    let mut tokens = segment
        .observations
        .iter()
        .map(|sample| sample.total_tokens)
        .collect::<Vec<_>>();
    durations.sort_unstable();
    tokens.sort_unstable();
    segment.duration_p50_ms = percentile(&durations, 50);
    segment.duration_p95_ms = percentile(&durations, 95);
    segment.total_tokens_p50 = percentile(&tokens, 50);
    segment.total_tokens_p95 = percentile(&tokens, 95);
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index = (values.len().saturating_sub(1) * percentile) / 100;
    values[index]
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::{
        outcome::{
            OutcomeIdentity, OutcomeObservation, OutcomeQuality, OutcomeTerminalClass,
            OutcomeTiming, OutcomeUsage, ProviderIdentity, RuntimeIdentity, StrategyIdentity,
            OUTCOME_SCHEMA_REVISION,
        },
        reality::EvidenceCompleteness,
        strategy::ExecutionCandidateKind,
    };

    fn outcome(execution_id: &str) -> ExecutionOutcome {
        ExecutionOutcome {
            identity: OutcomeIdentity {
                execution_id: execution_id.to_string(),
                session_id: "session".to_string(),
                turn_id: "turn".to_string(),
                terminal_generation: 1,
                paired_sample_id: None,
                task_id: None,
                mission_id: None,
                agent_id: None,
                team_id: None,
                execution_graph_ref: None,
            },
            runtime: RuntimeIdentity {
                workspace_key: "workspace".to_string(),
                runtime_revision: "test".to_string(),
                config_revision: "cfg".to_string(),
            },
            provider: Some(ProviderIdentity {
                registry_revision: Some(1),
                provider_name: "provider".to_string(),
                model: "model".to_string(),
                profile: Some("default".to_string()),
                protocol: Some("responses".to_string()),
                capabilities: std::collections::BTreeMap::new(),
            }),
            strategy: StrategyIdentity {
                decision_id: "decision".to_string(),
                policy_revision: "policy".to_string(),
                decision_source: "test".to_string(),
                selected_candidate: ExecutionCandidateKind::Direct,
                selected_pattern: "direct".to_string(),
            },
            timing: OutcomeTiming {
                started_at_ms: 100,
                completed_at_ms: 200,
                duration_ms: 100,
            },
            usage: OutcomeUsage {
                input_tokens: Some(10),
                output_tokens: Some(5),
                ..Default::default()
            },
            terminal: OutcomeTerminalClass::Succeeded("done".to_string()),
            quality: OutcomeQuality::estimate(8_000, "test", Some("calibration".to_string())),
            observation: OutcomeObservation {
                source: "test".to_string(),
                observed_at_ms: 200,
                freshness_ms: 0,
            },
            evidence_refs: Vec::new(),
            evidence_completeness: EvidenceCompleteness::Sufficient,
            schema_revision: OUTCOME_SCHEMA_REVISION,
        }
    }

    #[test]
    fn incremental_replay_and_restart_have_the_same_snapshot_hash() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = crate::execution_core::OutcomeService::new(Arc::clone(&store));
        service.record_terminal(&outcome("execution-1")).unwrap();
        service.record_terminal(&outcome("execution-2")).unwrap();

        let projector = OutcomeProjector::new(Arc::clone(&store));
        projector.project_available(128).unwrap();
        let online = projector.snapshot();
        let replay = projector.replay().unwrap();
        assert_eq!(
            serde_json::to_value(&*online).unwrap(),
            serde_json::to_value(&replay).unwrap()
        );

        let restarted = OutcomeProjector::new(store);
        assert_eq!(online.hash(), restarted.snapshot().hash());
    }

    #[test]
    fn projector_checkpoint_does_not_self_trigger_another_checkpoint() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = crate::execution_core::OutcomeService::new(Arc::clone(&store));
        service.record_terminal(&outcome("execution-1")).unwrap();
        let projector = OutcomeProjector::new(Arc::clone(&store));
        projector.project_available(128).unwrap();
        let checkpoint_count = store.list_stream(PROJECTOR_STREAM).unwrap().len();
        projector.project_available(128).unwrap();
        assert_eq!(
            store.list_stream(PROJECTOR_STREAM).unwrap().len(),
            checkpoint_count
        );
    }

    #[test]
    fn restart_replays_outcomes_committed_after_the_last_checkpoint() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = crate::execution_core::OutcomeService::new(Arc::clone(&store));
        service.record_terminal(&outcome("execution-1")).unwrap();
        let first = OutcomeProjector::new(Arc::clone(&store));
        first.project_available(128).unwrap();

        service.record_terminal(&outcome("execution-2")).unwrap();
        let restarted = OutcomeProjector::new(store);
        let snapshot = restarted.snapshot();
        assert_eq!(
            snapshot
                .segments
                .values()
                .map(|segment| segment.sample_count)
                .sum::<u64>(),
            2
        );
        assert_eq!(restarted.health().unwrap().lag_commits, 0);
    }

    #[test]
    fn malformed_outcome_is_dead_lettered_and_does_not_break_replay_or_restart() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        store
            .append(RuntimeEventInput {
                stream_id: "outcome:malformed".to_string(),
                scope: RuntimeEventScope::ExecutionGraph,
                kind: OUTCOME_EVENT_KIND.to_string(),
                status: Some("failed".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"schema_revision": 999}),
            })
            .unwrap();

        let projector = OutcomeProjector::new(Arc::clone(&store));
        assert!(projector.snapshot().segments.is_empty());
        assert_eq!(projector.snapshot().dlq_count, 1);
        assert!(projector.replay().unwrap().segments.is_empty());

        let restarted = OutcomeProjector::new(store);
        assert!(restarted.snapshot().segments.is_empty());
        assert_eq!(restarted.snapshot().dlq_count, 1);
    }
}
