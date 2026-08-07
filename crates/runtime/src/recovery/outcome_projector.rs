//! Recoverable, bounded read projection for canonical execution outcomes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use harness_contract::outcome::{
    ExecutionOutcome, OutcomeSegmentKey, StrategyExperienceKey, OUTCOME_SCHEMA_REVISION,
};
use harness_contract::strategy::ExecutionCandidateKind;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    CancellationToken, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    RuntimeTransactionEventInput,
};

use crate::execution_core::outcome_service::OUTCOME_EVENT_KIND;

const PROJECTOR_STREAM: &str = "outcome-projector";
const PROJECTOR_ID: &str = "projector:outcome";
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
    verification_blocked: bool,
    context_pressure: bool,
    coordination_cost_ms: u64,
    observed_at_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyExperienceSnapshot {
    pub key: Option<StrategyExperienceKey>,
    pub sample_count: u64,
    pub success_count: u64,
    pub verification_block_count: u64,
    pub context_pressure_count: u64,
    pub evidence_complete_count: u64,
    pub quality_observed_count: u64,
    pub paired_comparison_count: u64,
    pub positive_lift_count: u64,
    pub last_observed_at_ms: u64,
    pub duration_p50_ms: u64,
    pub total_tokens_p50: u64,
    pub coordination_cost_p50_ms: u64,
    #[serde(default)]
    observations: Vec<OutcomeObservationSample>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeReadSnapshot {
    pub revision: u64,
    pub source_cursor: u64,
    pub projected_at_ms: u64,
    pub segments: BTreeMap<String, OutcomeSegmentSnapshot>,
    #[serde(default)]
    pub strategy_experience: BTreeMap<String, StrategyExperienceSnapshot>,
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

    #[must_use]
    pub fn strategy_experience(
        &self,
        key: &StrategyExperienceKey,
    ) -> Option<&StrategyExperienceSnapshot> {
        let key = serde_json::to_string(key).ok()?;
        self.strategy_experience.get(&key)
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
        // Startup restores only the latest compact checkpoint. Catch-up runs
        // in the projector worker after Runtime composition, so an unbounded
        // historical replay can never delay Gateway readiness.
        let snapshot = restore_latest_snapshot(&event_store).unwrap_or_default();
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
        let mut source_cursor = 0;
        loop {
            let batches = self
                .event_store
                .events_after_cursor(cursor, PROJECTOR_BATCH)
                .map_err(|error| error.to_string())?;
            if batches.is_empty() {
                break;
            }
            for batch in batches {
                let has_external_event = batch
                    .events
                    .iter()
                    .any(|event| event.stream_id != PROJECTOR_STREAM);
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
                if has_external_event {
                    source_cursor = batch.commit_cursor;
                }
            }
        }
        replay.source_cursor = source_cursor;
        replay.projected_at_ms = self.snapshot().projected_at_ms;
        replay.revision = self.snapshot().revision;
        replay.dlq_count = self.snapshot().dlq_count;
        Ok(replay)
    }

    fn checkpoint(&self, snapshot: &OutcomeReadSnapshot) -> Result<(), String> {
        let source_cursor = snapshot.source_cursor;
        self.event_store
            .put_projection_checkpoint(
                PROJECTOR_ID,
                source_cursor,
                &serde_json::json!({
                    "checkpoint": OutcomeProjectionCheckpoint {
                        source_cursor,
                        snapshot_revision: snapshot.revision,
                        projected_at_ms: snapshot.projected_at_ms,
                    },
                    "snapshot": snapshot,
                    "snapshot_hash": snapshot.hash(),
                }),
                now_ms(),
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
        .projection_checkpoint(PROJECTOR_ID)
        .map_err(|error| error.to_string())?
        .and_then(|checkpoint| checkpoint.payload.get("snapshot").cloned())
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| error.to_string())
        .map(Option::unwrap_or_default)
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
        ) && !outcome.evidence_refs.is_empty(),
        verification_blocked: outcome.strategy_feedback.verification_blocked,
        context_pressure: outcome.strategy_feedback.context_pressure,
        coordination_cost_ms: outcome.strategy_feedback.coordination_cost_ms,
        observed_at_ms: outcome.observation.observed_at_ms,
    });
    if segment.observations.len() > MAX_OBSERVATIONS_PER_SEGMENT {
        segment.observations.remove(0);
    }
    recompute_segment(segment);
    if let Some(experience_key) = StrategyExperienceKey::from_outcome(&outcome) {
        reduce_strategy_experience(snapshot, experience_key, &outcome);
    }
}

fn reduce_strategy_experience(
    snapshot: &mut OutcomeReadSnapshot,
    key: StrategyExperienceKey,
    outcome: &ExecutionOutcome,
) {
    let key_id = serde_json::to_string(&key).unwrap_or_else(|_| "invalid-experience".to_string());
    let outcome_id = format!(
        "{}:{}:{}",
        outcome.identity.execution_id,
        outcome.identity.terminal_generation,
        outcome.schema_revision
    );
    let experience = snapshot
        .strategy_experience
        .entry(key_id)
        .or_insert_with(|| StrategyExperienceSnapshot {
            key: Some(key.clone()),
            ..Default::default()
        });
    if experience
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
    experience.observations.push(OutcomeObservationSample {
        outcome_id,
        duration_ms: outcome.timing.duration_ms,
        total_tokens,
        succeeded: outcome.terminal.is_success(),
        terminal_class: outcome.terminal.class_name().to_string(),
        paired_sample_id: outcome.identity.paired_sample_id.clone(),
        schema_revision: outcome.schema_revision,
        quality_observed: !matches!(
            outcome.quality,
            harness_contract::outcome::OutcomeQuality::Unknown
        ),
        quality_bp: match outcome.quality {
            harness_contract::outcome::OutcomeQuality::Unknown => None,
            harness_contract::outcome::OutcomeQuality::Estimate { value_bp, .. } => Some(value_bp),
        },
        evidence_complete: matches!(
            outcome.evidence_completeness,
            harness_contract::reality::EvidenceCompleteness::Sufficient
        ) && !outcome.evidence_refs.is_empty(),
        verification_blocked: outcome.strategy_feedback.verification_blocked,
        context_pressure: outcome.strategy_feedback.context_pressure,
        coordination_cost_ms: outcome.strategy_feedback.coordination_cost_ms,
        observed_at_ms: outcome.observation.observed_at_ms,
    });
    if experience.observations.len() > MAX_OBSERVATIONS_PER_SEGMENT {
        experience.observations.remove(0);
    }
    recompute_experience(experience);
    recompute_paired_lift(snapshot, &key);
}

fn recompute_experience(experience: &mut StrategyExperienceSnapshot) {
    experience.sample_count = experience.observations.len() as u64;
    experience.success_count = experience
        .observations
        .iter()
        .filter(|sample| sample.succeeded)
        .count() as u64;
    experience.verification_block_count = experience
        .observations
        .iter()
        .filter(|sample| sample.verification_blocked)
        .count() as u64;
    experience.context_pressure_count = experience
        .observations
        .iter()
        .filter(|sample| sample.context_pressure)
        .count() as u64;
    experience.evidence_complete_count = experience
        .observations
        .iter()
        .filter(|sample| sample.evidence_complete)
        .count() as u64;
    experience.quality_observed_count = experience
        .observations
        .iter()
        .filter(|sample| sample.quality_observed)
        .count() as u64;
    experience.last_observed_at_ms = experience
        .observations
        .iter()
        .map(|sample| sample.observed_at_ms)
        .max()
        .unwrap_or_default();
    let mut durations = experience
        .observations
        .iter()
        .map(|sample| sample.duration_ms)
        .collect::<Vec<_>>();
    let mut tokens = experience
        .observations
        .iter()
        .map(|sample| sample.total_tokens)
        .collect::<Vec<_>>();
    let mut coordination = experience
        .observations
        .iter()
        .map(|sample| sample.coordination_cost_ms)
        .collect::<Vec<_>>();
    durations.sort_unstable();
    tokens.sort_unstable();
    coordination.sort_unstable();
    experience.duration_p50_ms = percentile(&durations, 50);
    experience.total_tokens_p50 = percentile(&tokens, 50);
    experience.coordination_cost_p50_ms = percentile(&coordination, 50);
}

fn recompute_paired_lift(snapshot: &mut OutcomeReadSnapshot, changed_key: &StrategyExperienceKey) {
    let team_key = changed_key.with_candidate(ExecutionCandidateKind::Team);
    let Ok(team_key_id) = serde_json::to_string(&team_key) else {
        return;
    };
    let baseline_samples = [
        ExecutionCandidateKind::Direct,
        ExecutionCandidateKind::ParallelTools,
    ]
    .into_iter()
    .filter_map(|candidate| {
        let key = team_key.with_candidate(candidate);
        let key_id = serde_json::to_string(&key).ok()?;
        snapshot.strategy_experience.get(&key_id)
    })
    .flat_map(|experience| experience.observations.iter())
    .filter_map(|sample| {
        sample
            .paired_sample_id
            .as_ref()
            .map(|id| (id.clone(), sample.clone()))
    })
    .fold(
        BTreeMap::<String, Vec<OutcomeObservationSample>>::new(),
        |mut samples, (pair_id, sample)| {
            samples.entry(pair_id).or_default().push(sample);
            samples
        },
    );
    let Some(team) = snapshot.strategy_experience.get_mut(&team_key_id) else {
        return;
    };
    let comparisons = team
        .observations
        .iter()
        .filter_map(|sample| {
            let pair_id = sample.paired_sample_id.as_ref()?;
            let baselines = baseline_samples.get(pair_id)?;
            let Some(team_quality) = sample.quality_bp else {
                return None;
            };
            if !sample.evidence_complete
                || baselines.is_empty()
                || baselines
                    .iter()
                    .any(|baseline| !baseline.evidence_complete || baseline.quality_bp.is_none())
            {
                return None;
            }
            // Multiple baselines for one pair are treated conservatively:
            // Team must prove lift against every valid Direct/Parallel sample.
            Some(baselines.iter().all(|baseline| {
                let baseline_quality = baseline.quality_bp.expect("validated above");
                let quality_delta = i32::from(team_quality) - i32::from(baseline_quality);
                let speed_channel = sample.duration_ms.saturating_mul(100)
                    <= baseline.duration_ms.saturating_mul(80)
                    && quality_delta >= -200;
                let quality_channel = quality_delta >= 1_000
                    && sample.duration_ms.saturating_mul(100)
                        <= baseline.duration_ms.saturating_mul(110);
                speed_channel || quality_channel
            }))
        })
        .collect::<Vec<_>>();
    team.paired_comparison_count = comparisons.len() as u64;
    team.positive_lift_count = comparisons.iter().filter(|positive| **positive).count() as u64;
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
        reality::{EvidenceCompleteness, EvidenceRef},
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
            strategy_feedback: harness_contract::outcome::OutcomeStrategyFeedback {
                workload: Some(
                    harness_contract::strategy::StrategyWorkloadFingerprint::from_understanding(
                        &harness_contract::strategy::understand(
                            &harness_contract::strategy::StrategyInput::from_prompt(
                                "bounded test task",
                            ),
                        ),
                        false,
                    ),
                ),
                evaluation_environment: "production".to_string(),
                ..Default::default()
            },
            evidence_refs: vec![EvidenceRef::observed("test_report", execution_id)],
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
    fn revision_one_outcome_without_strategy_feedback_remains_readable() {
        let mut legacy = serde_json::to_value(outcome("legacy-v1")).unwrap();
        legacy.as_object_mut().unwrap().remove("strategy_feedback");
        let restored: ExecutionOutcome = serde_json::from_value(legacy).unwrap();
        assert_eq!(restored.schema_revision, 1);
        assert_eq!(
            restored.strategy_feedback,
            harness_contract::outcome::OutcomeStrategyFeedback::default()
        );
        assert!(StrategyExperienceKey::from_outcome(&restored).is_none());
    }

    #[test]
    fn projector_checkpoint_is_mutable_and_does_not_self_trigger() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = crate::execution_core::OutcomeService::new(Arc::clone(&store));
        service.record_terminal(&outcome("execution-1")).unwrap();
        let commits = store.subscribe_commits();
        let projector = OutcomeProjector::new(Arc::clone(&store));
        projector.project_available(128).unwrap();
        let commit_cursor = *commits.borrow();
        let event_count = store.events_after_cursor(0, usize::MAX).unwrap().len();
        assert_eq!(
            store
                .projection_checkpoint(PROJECTOR_ID)
                .unwrap()
                .expect("checkpoint")
                .source_cursor,
            commit_cursor
        );
        projector.project_available(128).unwrap();
        assert_eq!(*commits.borrow(), commit_cursor);
        assert_eq!(
            store.events_after_cursor(0, usize::MAX).unwrap().len(),
            event_count
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
        assert_eq!(
            restarted
                .snapshot()
                .segments
                .values()
                .map(|segment| segment.sample_count)
                .sum::<u64>(),
            1
        );
        restarted.project_available(128).unwrap();
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
                scope: RuntimeEventScope::Task,
                kind: OUTCOME_EVENT_KIND.to_string(),
                status: Some("failed".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"schema_revision": 999}),
            })
            .unwrap();

        let projector = OutcomeProjector::new(Arc::clone(&store));
        assert!(projector.snapshot().segments.is_empty());
        assert_eq!(projector.snapshot().dlq_count, 0);
        projector.project_available(128).unwrap();
        assert_eq!(projector.snapshot().dlq_count, 1);
        assert!(projector.replay().unwrap().segments.is_empty());

        let restarted = OutcomeProjector::new(store);
        assert!(restarted.snapshot().segments.is_empty());
        assert_eq!(restarted.snapshot().dlq_count, 1);
    }

    #[test]
    fn strategy_experience_is_exactly_isolated_by_every_scope_dimension() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = crate::execution_core::OutcomeService::new(Arc::clone(&store));
        let baseline = outcome("scope-baseline");
        let baseline_key = StrategyExperienceKey::from_outcome(&baseline).unwrap();
        service.record_terminal(&baseline).unwrap();

        let mut variants = Vec::new();
        let mut workspace = outcome("scope-workspace");
        workspace.runtime.workspace_key = "other-workspace".to_string();
        variants.push(workspace);
        let mut workload = outcome("scope-workload");
        workload
            .strategy_feedback
            .workload
            .as_mut()
            .unwrap()
            .responsibility_domains = 7;
        variants.push(workload);
        let mut config = outcome("scope-config");
        config.runtime.config_revision = "other-config".to_string();
        variants.push(config);
        let mut provider = outcome("scope-provider");
        provider.provider.as_mut().unwrap().provider_name = "other-provider".to_string();
        variants.push(provider);
        let mut model = outcome("scope-model");
        model.provider.as_mut().unwrap().model = "other-model".to_string();
        variants.push(model);
        let mut environment = outcome("scope-environment");
        environment.strategy_feedback.evaluation_environment = "harness_evaluation".to_string();
        variants.push(environment);
        for variant in &variants {
            service.record_terminal(variant).unwrap();
        }

        let projector = OutcomeProjector::new(store);
        projector.project_available(128).unwrap();
        let snapshot = projector.snapshot();
        assert_eq!(
            snapshot
                .strategy_experience(&baseline_key)
                .expect("baseline scope")
                .sample_count,
            1
        );
        assert_eq!(snapshot.strategy_experience.len(), variants.len() + 1);
        for variant in variants {
            let key = StrategyExperienceKey::from_outcome(&variant).unwrap();
            assert_ne!(key, baseline_key);
            assert_eq!(
                snapshot
                    .strategy_experience(&key)
                    .expect("isolated scope")
                    .sample_count,
                1
            );
        }
    }

    #[test]
    fn only_paired_quality_complete_team_evidence_proves_positive_lift() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = crate::execution_core::OutcomeService::new(Arc::clone(&store));
        let mut direct = outcome("paired-direct");
        direct.identity.paired_sample_id = Some("pair-1".to_string());
        direct.timing.duration_ms = 100;
        let mut team = outcome("paired-team");
        team.identity.paired_sample_id = Some("pair-1".to_string());
        team.strategy.selected_candidate = ExecutionCandidateKind::Team;
        team.strategy.selected_pattern = "collaborate".to_string();
        team.timing.duration_ms = 75;
        service.record_terminal(&direct).unwrap();
        service.record_terminal(&team).unwrap();

        let projector = OutcomeProjector::new(Arc::clone(&store));
        projector.project_available(128).unwrap();
        let key = StrategyExperienceKey::from_outcome(&team).unwrap();
        let snapshot = projector.snapshot();
        let experience = snapshot.strategy_experience(&key).expect("team experience");
        assert_eq!(experience.paired_comparison_count, 1);
        assert_eq!(experience.positive_lift_count, 1);

        let mut unpaired = outcome("unpaired-team");
        unpaired.strategy.selected_candidate = ExecutionCandidateKind::Team;
        unpaired.strategy.selected_pattern = "collaborate".to_string();
        unpaired.timing.duration_ms = 1;
        service.record_terminal(&unpaired).unwrap();
        projector.project_available(128).unwrap();
        let snapshot = projector.snapshot();
        let experience = snapshot.strategy_experience(&key).expect("team experience");
        assert_eq!(experience.paired_comparison_count, 1);
        assert_eq!(experience.positive_lift_count, 1);
    }

    #[test]
    fn missing_durable_evidence_cannot_prove_team_lift() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = crate::execution_core::OutcomeService::new(Arc::clone(&store));
        let mut direct = outcome("incomplete-direct");
        direct.identity.paired_sample_id = Some("pair-incomplete".to_string());
        let mut team = outcome("incomplete-team");
        team.identity.paired_sample_id = Some("pair-incomplete".to_string());
        team.strategy.selected_candidate = ExecutionCandidateKind::Team;
        team.strategy.selected_pattern = "collaborate".to_string();
        team.timing.duration_ms = 1;
        team.evidence_refs.clear();
        service.record_terminal(&direct).unwrap();
        service.record_terminal(&team).unwrap();

        let projector = OutcomeProjector::new(store);
        projector.project_available(128).unwrap();
        let key = StrategyExperienceKey::from_outcome(&team).unwrap();
        let snapshot = projector.snapshot();
        let experience = snapshot.strategy_experience(&key).expect("team experience");
        assert_eq!(experience.paired_comparison_count, 0);
        assert_eq!(experience.positive_lift_count, 0);
    }
}
