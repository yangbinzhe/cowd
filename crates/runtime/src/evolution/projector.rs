//! Supervised projection from execution evidence to evolution signals.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use harness_contract::outcome::{ExecutionOutcome, OutcomeTerminalClass};
use harness_contract::reality::EvidenceRef;
use sha2::{Digest, Sha256};

use super::{
    EvolutionDiscoveryService, EvolutionSignal, EvolutionSignalScope, EvolutionSignalSeverity,
    EvolutionSignalSource, EvolutionSignalType,
};
use crate::execution_core::outcome_service::OUTCOME_EVENT_KIND;
use crate::{
    AppendTransactionRequest, CancellationToken, CommittedEventBatch, DurableRuntimeEvent,
    ExpectedStreamRevision, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope,
    RuntimeEventStore, RuntimeTransactionEventInput,
};

const PROJECTOR_STREAM: &str = "evolution-signal-projector";
const PROJECTOR_ID: &str = "projector:evolution-case:v2";
const LEGACY_BOOTSTRAP_ID: &str = "projector:evolution-case-bootstrap:v2";
const PROJECTOR_BATCH: usize = 128;
// Evolution is explicitly lower priority than foreground execution. Keep each
// supervised pass small and leave a scheduling window between non-empty
// passes; the durable cursor provides eventual catch-up without turning an
// evidence burst into SQLite lock pressure on the active mission.
const PROJECTOR_WORKER_BATCH: usize = 8;
const MAX_SOURCE_RETRIES: usize = 3;
const REPAIR_BATCH: usize = 32;
// One Runtime transaction is already hard-capped at these exact limits by the
// canonical EventStore. Reading one commit at a time therefore makes the
// projector's peak source slice independently bounded by the same contract.
const MAX_SCAN_EVENTS: usize = 10_000;
const MAX_SCAN_BYTES: usize = 32 * 1024 * 1024;
const MAX_SCAN_WALL: Duration = Duration::from_millis(50);
const PROJECTOR_ACTIVE_POLL: Duration = Duration::from_secs(1);
const PROJECTOR_IDLE_POLL: Duration = Duration::from_secs(1);
const FAILED_KIND: &str = "evolution.signal.projector.failed.v1";
const RECOVERED_KIND: &str = "evolution.signal.projector.recovered.v1";
const FAILURE_INDEX_KIND: &str = "evolution.signal.projector.failure_index.v2";
const FAILURE_CATALOG_PAGE_KIND: &str = "evolution.signal.projector.failure_page.frozen.v2";
const FAILURE_CATALOG_PAGE_SIZE: usize = 1_024;
const AGENT_FAILURE_WINDOW_PREFIX: &str = "projector:evolution-agent-failure-window:v2:";
const AGENT_FAILURE_WINDOW_SIZE: usize = 128;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct AgentFailureWindow {
    observations: Vec<AgentFailureObservation>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AgentFailureObservation {
    evaluation_id: String,
    run_id: String,
    succeeded: bool,
    failure: Option<String>,
    created_at_ms: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct ProjectorFailureIndex {
    unresolved_count: u64,
    #[serde(default)]
    head_page: u64,
    #[serde(default)]
    head_offset: usize,
    #[serde(default)]
    tail_page: u64,
    #[serde(default)]
    tail_source_event_ids: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ProjectorFailureCatalogPage {
    page: u64,
    source_event_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct EvolutionProjectorHealth {
    pub source_cursor: u64,
    pub latest_commit_cursor: u64,
    pub lag_commits: u64,
    pub dead_letter_count: usize,
    pub worker_running: bool,
    pub consecutive_failures: u32,
    pub scan_commit_limit: usize,
    pub scan_event_limit: usize,
    pub scan_byte_limit: usize,
    pub scan_wall_limit_ms: u64,
}

pub(crate) struct EvolutionSignalProjector {
    event_store: Arc<RuntimeEventStore>,
    discovery: Arc<EvolutionDiscoveryService>,
    cancellation: CancellationToken,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    consecutive_failures: AtomicU32,
}

impl EvolutionSignalProjector {
    #[must_use]
    pub(crate) fn new(
        event_store: Arc<RuntimeEventStore>,
        discovery: Arc<EvolutionDiscoveryService>,
    ) -> Self {
        Self {
            event_store,
            discovery,
            cancellation: CancellationToken::new(),
            worker: Mutex::new(None),
            consecutive_failures: AtomicU32::new(0),
        }
    }

    pub(crate) fn start(self: &Arc<Self>) {
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
            tokio::select! {
                _ = projector.cancellation.cancelled() => return,
                _ = tokio::time::sleep(PROJECTOR_IDLE_POLL) => {}
            }
            loop {
                let pass = {
                    let projector = Arc::clone(&projector);
                    tokio::task::spawn_blocking(move || projector.run_once(PROJECTOR_WORKER_BATCH))
                        .await
                };
                let processed = match pass {
                    Ok(Ok(processed)) => {
                        projector.consecutive_failures.store(0, Ordering::Relaxed);
                        processed
                    }
                    Ok(Err(error)) => {
                        let failures = projector
                            .consecutive_failures
                            .fetch_add(1, Ordering::Relaxed)
                            .saturating_add(1);
                        tracing::warn!(%error, failures, "evolution case projector pass failed");
                        0
                    }
                    Err(error) => {
                        let failures = projector
                            .consecutive_failures
                            .fetch_add(1, Ordering::Relaxed)
                            .saturating_add(1);
                        tracing::warn!(%error, failures, "evolution case projector worker failed");
                        0
                    }
                };
                let failures = projector.consecutive_failures.load(Ordering::Relaxed);
                let failure_backoff = (failures > 0).then(|| {
                    Duration::from_millis(
                        100_u64.saturating_mul(1_u64 << failures.min(8)).min(30_000),
                    )
                });
                let delay = failure_backoff.unwrap_or(if processed > 0 {
                    PROJECTOR_ACTIVE_POLL
                } else {
                    PROJECTOR_IDLE_POLL
                });
                tokio::select! {
                    _ = projector.cancellation.cancelled() => break,
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        }));
    }

    pub(crate) async fn shutdown(&self) {
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

    pub(crate) fn health(&self) -> Result<EvolutionProjectorHealth, String> {
        let (source_cursor, latest_commit_cursor, lag_commits, dead_letter_count) = self
            .event_store
            .run_projection_work(crate::RuntimeProjectionWorkClass::Background, || {
                let source_cursor = self.cursor()?;
                let (latest_commit_cursor, lag_commits) = self.source_lag(source_cursor);
                let dead_letter_count =
                    usize::try_from(self.failure_index()?.unresolved_count).unwrap_or(usize::MAX);
                Ok::<_, String>((
                    source_cursor,
                    latest_commit_cursor,
                    lag_commits,
                    dead_letter_count,
                ))
            })?;
        let worker_running = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|worker| !worker.is_finished());
        Ok(EvolutionProjectorHealth {
            source_cursor,
            latest_commit_cursor,
            lag_commits,
            dead_letter_count,
            worker_running,
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            scan_commit_limit: PROJECTOR_BATCH,
            scan_event_limit: MAX_SCAN_EVENTS,
            scan_byte_limit: MAX_SCAN_BYTES,
            scan_wall_limit_ms: MAX_SCAN_WALL.as_millis() as u64,
        })
    }

    /// Measure lag only against commits that contain Runtime source events.
    ///
    /// The projector records its output and checkpoint in the same event
    /// store. Counting those Evolution-only commits would make a healthy idle
    /// projector report permanent lag after every successful pass.
    fn source_lag(&self, source_cursor: u64) -> (u64, u64) {
        let upper_bound = self.event_store.current_commit_cursor();
        (upper_bound, upper_bound.saturating_sub(source_cursor))
    }

    /// Consume a bounded number of non-Evolution source commits.
    ///
    /// Evolution output and checkpoint commits do not consume the source
    /// budget, so a small batch cannot starve execution events behind the
    /// projector's own writes.
    pub(crate) fn run_once(&self, max_commits: usize) -> Result<usize, String> {
        let source_cursor = self
            .event_store
            .run_projection_work(crate::RuntimeProjectionWorkClass::Background, || {
                self.cursor()
            })?;
        let class = if source_cursor == 0 {
            crate::RuntimeProjectionWorkClass::Recovery
        } else {
            crate::RuntimeProjectionWorkClass::Background
        };
        self.event_store
            .run_projection_work(class, || self.run_once_scoped(max_commits))
    }

    fn run_once_scoped(&self, max_commits: usize) -> Result<usize, String> {
        self.bootstrap_legacy_signals(PROJECTOR_BATCH)?;
        self.discovery.resume_auto_cases(REPAIR_BATCH)?;
        self.repair_dead_letters(REPAIR_BATCH)?;
        let cursor = self.cursor()?;
        let max_commits = max_commits.max(1);
        let mut scan_cursor = cursor;
        let mut last_scanned_cursor = cursor;
        let mut processed = 0;
        let mut scanned_commits = 0_usize;
        let mut scanned_events = 0_usize;
        let mut scanned_bytes = 0_usize;
        let started = Instant::now();
        'scan: loop {
            if scanned_commits >= max_commits || started.elapsed() >= MAX_SCAN_WALL {
                break;
            }
            let batches = self
                .event_store
                .events_after_cursor(scan_cursor, 1)
                .map_err(|error| error.to_string())?;
            if batches.is_empty() {
                break;
            }
            for batch in batches {
                let batch_events = batch.events.len();
                let batch_bytes = batch.events.iter().fold(0_usize, |total, event| {
                    total.saturating_add(serde_json::to_vec(event).map_or(0, |bytes| bytes.len()))
                });
                if scanned_commits > 0
                    && (scanned_events.saturating_add(batch_events) > MAX_SCAN_EVENTS
                        || scanned_bytes.saturating_add(batch_bytes) > MAX_SCAN_BYTES
                        || started.elapsed() >= MAX_SCAN_WALL)
                {
                    break 'scan;
                }
                scan_cursor = batch.commit_cursor;
                last_scanned_cursor = batch.commit_cursor;
                scanned_commits += 1;
                scanned_events = scanned_events.saturating_add(batch_events);
                scanned_bytes = scanned_bytes.saturating_add(batch_bytes);
                if is_projector_output_only(&batch) {
                    if scanned_commits == max_commits {
                        break 'scan;
                    }
                    continue;
                }
                let has_agent_evaluation = batch
                    .events
                    .iter()
                    .any(|event| event.kind == "agent.run_evaluated");
                for event in batch
                    .events
                    .iter()
                    .filter(|event| !(has_agent_evaluation && event.kind == "agent.terminal"))
                {
                    self.process_source(event, batch.commit_cursor)?;
                }
                processed += 1;
                if scanned_commits == max_commits {
                    break 'scan;
                }
            }
        }
        if last_scanned_cursor > cursor {
            self.checkpoint(last_scanned_cursor)?;
        }
        Ok(processed)
    }

    fn bootstrap_legacy_signals(&self, limit: usize) -> Result<usize, String> {
        let checkpoint = self
            .event_store
            .projection_checkpoint(LEGACY_BOOTSTRAP_ID)
            .map_err(|error| error.to_string())?;
        let after_position = checkpoint.as_ref().and_then(|checkpoint| {
            checkpoint
                .payload
                .get("transaction_index")
                .and_then(serde_json::Value::as_u64)
                .and_then(|index| u32::try_from(index).ok())
                .map(|index| (checkpoint.source_cursor, index))
        });
        let events = self
            .event_store
            .list_scope_kind_page_asc(
                RuntimeEventScope::Evolution,
                "evolution.signal.recorded.v1",
                after_position,
                limit.clamp(1, PROJECTOR_BATCH),
            )
            .map_err(|error| error.to_string())?;
        let Some(last) = events.last() else {
            return Ok(0);
        };
        for event in &events {
            let signal = event
                .payload
                .get("signal")
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "legacy evolution signal {} omitted typed payload",
                        event.event_id
                    )
                })
                .and_then(|value| {
                    serde_json::from_value::<EvolutionSignal>(value)
                        .map_err(|error| error.to_string())
                })?;
            self.discovery.record_signal(signal)?;
        }
        self.event_store
            .compare_and_put_projection_checkpoint(
                LEGACY_BOOTSTRAP_ID,
                last.commit_cursor,
                checkpoint
                    .as_ref()
                    .map_or(0, |checkpoint| checkpoint.revision),
                &serde_json::json!({
                    "transaction_index": last.transaction_index,
                    "legacy_signal_count": events.len(),
                }),
                now_ms(),
            )
            .map_err(|error| error.to_string())?;
        Ok(events.len())
    }

    fn process_source(
        &self,
        event: &DurableRuntimeEvent,
        source_cursor: u64,
    ) -> Result<(), String> {
        let mut last_error = None;
        for _ in 0..MAX_SOURCE_RETRIES {
            match self.project_source(event) {
                Ok(_) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
        }
        self.dead_letter(
            event,
            source_cursor,
            last_error
                .as_deref()
                .unwrap_or("unknown projection failure"),
        )
    }

    fn project_source(&self, event: &DurableRuntimeEvent) -> Result<bool, String> {
        let signal = if event.kind == OUTCOME_EVENT_KIND {
            outcome_signal(event)?
        } else if event.kind == "agent.run_evaluated" {
            self.repeated_agent_failure_signal(event)?
        } else {
            signal_from_source_event(event)
        };
        let Some(signal) = signal else {
            return Ok(false);
        };
        self.discovery.record_signal(signal).map(|_| true)
    }

    fn unresolved_dead_letters(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        let index = self.failure_index()?;
        let mut page = index.head_page;
        let mut offset = index.head_offset;
        let mut failures = Vec::with_capacity(limit);
        while failures.len() < limit && page <= index.tail_page {
            let catalog = self.failure_catalog_page(&index, page)?;
            if offset > catalog.source_event_ids.len() {
                return Err("evolution projector failure cursor is corrupt".to_string());
            }
            for source_event_id in catalog.source_event_ids.iter().skip(offset) {
                let failure = self
                    .event_store
                    .event_by_idempotency_key(
                        PROJECTOR_STREAM,
                        &format!("dead-letter:{source_event_id}"),
                    )
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| {
                        format!(
                            "evolution projector failure queue references missing source: {source_event_id}"
                        )
                    })?;
                failures.push(failure);
                offset = offset.saturating_add(1);
                if failures.len() == limit {
                    break;
                }
            }
            if offset >= catalog.source_event_ids.len() {
                if page >= index.tail_page {
                    break;
                }
                page = page.saturating_add(1);
                offset = 0;
            }
        }
        Ok(failures)
    }

    fn failure_catalog_page(
        &self,
        index: &ProjectorFailureIndex,
        page: u64,
    ) -> Result<ProjectorFailureCatalogPage, String> {
        if page == index.tail_page {
            return Ok(ProjectorFailureCatalogPage {
                page,
                source_event_ids: index.tail_source_event_ids.clone(),
            });
        }
        self.event_store
            .latest_for_stream(&failure_catalog_stream(page))?
            .filter(|event| event.kind == FAILURE_CATALOG_PAGE_KIND)
            .and_then(|event| event.payload.get("page").cloned())
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("evolution projector failure page {page} is missing"))
    }

    fn failure_index(&self) -> Result<ProjectorFailureIndex, String> {
        self.event_store
            .latest_for_stream_kind(PROJECTOR_STREAM, FAILURE_INDEX_KIND)?
            .and_then(|event| event.payload.get("index").cloned())
            .map(serde_json::from_value)
            .transpose()
            .map(|index| index.unwrap_or_default())
            .map_err(|error| error.to_string())
    }

    fn repair_dead_letters(&self, limit: usize) -> Result<usize, String> {
        let Some(limit) = NonZeroUsize::new(limit) else {
            return Ok(0);
        };
        let failures = self.unresolved_dead_letters(limit.get())?;
        let mut repaired = 0;
        for failure in failures {
            let source_cursor = failure
                .payload
                .get("source_cursor")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    format!(
                        "evolution projector failure {} omitted source_cursor",
                        failure.event_id
                    )
                })?;
            let source_event_id = failure
                .payload
                .get("source_event_id")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    format!(
                        "evolution projector failure {} omitted source_event_id",
                        failure.event_id
                    )
                })?;
            let source = self
                .event_store
                .events_after_cursor(source_cursor.saturating_sub(1), 1)
                .map_err(|error| error.to_string())?
                .into_iter()
                .find(|batch| batch.commit_cursor == source_cursor)
                .and_then(|batch| {
                    batch
                        .events
                        .into_iter()
                        .find(|event| event.event_id == source_event_id)
                })
                .ok_or_else(|| {
                    format!(
                        "evolution projector source event {source_event_id} at cursor {source_cursor} is unavailable"
                    )
                })?;

            let projected = self.project_source(&source)?;
            self.mark_recovered(&failure, &source, projected)?;
            repaired += 1;
        }
        Ok(repaired)
    }

    fn mark_recovered(
        &self,
        failure: &DurableRuntimeEvent,
        source: &DurableRuntimeEvent,
        projected: bool,
    ) -> Result<(), String> {
        let key = format!("recovered:{}", failure.event_id);
        if self
            .event_store
            .event_by_idempotency_key(PROJECTOR_STREAM, &key)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Ok(());
        }
        for _ in 0..32 {
            let mut index = self.failure_index()?;
            let head = self
                .failure_catalog_page(&index, index.head_page)?
                .source_event_ids
                .get(index.head_offset)
                .cloned();
            if head.as_deref() != Some(source.event_id.as_str()) {
                if head.is_none() {
                    return Ok(());
                }
                return Err(format!(
                    "evolution projector recovery is out of queue order: expected {}, received {}",
                    head.unwrap_or_default(),
                    source.event_id
                ));
            }
            index.unresolved_count = index.unresolved_count.saturating_sub(1);
            index.head_offset = index.head_offset.saturating_add(1);
            if index.head_page < index.tail_page && index.head_offset >= FAILURE_CATALOG_PAGE_SIZE {
                index.head_page = index.head_page.saturating_add(1);
                index.head_offset = 0;
            }
            let expected_revision = self
                .event_store
                .stream_revision(PROJECTOR_STREAM)
                .map_err(|error| error.to_string())?;
            match self.event_store.append_batch_if_revision(
                PROJECTOR_STREAM,
                expected_revision,
                format!(
                    "evolution-projector-recovered:{}:{expected_revision}",
                    failure.event_id
                ),
                vec![
                    projector_event(
                        RECOVERED_KIND,
                        Some(if projected {
                            "recovered"
                        } else {
                            "no_longer_applicable"
                        }),
                        vec![
                            RuntimeEventRef {
                                kind: "projector_failure".to_string(),
                                id: failure.event_id.clone(),
                            },
                            RuntimeEventRef {
                                kind: "source_event".to_string(),
                                id: source.event_id.clone(),
                            },
                        ],
                        serde_json::json!({
                            "failure_event_id": failure.event_id,
                            "source_event_id": source.event_id,
                            "source_cursor": source.commit_cursor,
                            "projected": projected,
                        }),
                        key.clone(),
                    ),
                    projector_event(
                        FAILURE_INDEX_KIND,
                        Some("indexed"),
                        Vec::new(),
                        serde_json::json!({"index": index}),
                        format!("failure-index-recovered:{}", source.event_id),
                    ),
                ],
            ) {
                Ok(_) => return Ok(()),
                Err(crate::RuntimeEventStoreError::StaleRevision { .. }) => continue,
                Err(error) => return Err(error.to_string()),
            }
        }
        Err("projector failure index remained contended during recovery".to_string())
    }

    /// Aggregate immutable Agent evaluation events before emitting a signal.
    ///
    /// This preserves the useful repeated-failure detector that previously
    /// lived in Agent Runtime while keeping all discovery writes behind the
    /// single bounded projector. One failed run cannot create a noisy
    /// proposal, and the deterministic key collapses replay into one chain.
    fn repeated_agent_failure_signal(
        &self,
        source_event: &DurableRuntimeEvent,
    ) -> Result<Option<EvolutionSignal>, String> {
        let trigger = source_event
            .payload
            .get("evaluation")
            .cloned()
            .ok_or_else(|| "agent evaluation source is missing its typed payload".to_string())
            .and_then(|value| {
                serde_json::from_value::<crate::AgentRunEvaluation>(value)
                    .map_err(|error| error.to_string())
            })?;
        if trigger.is_success() {
            return Ok(None);
        }
        let definition_id =
            harness_contract::agent::AgentDefinitionId::try_from(trigger.definition_id.as_str())
                .map_err(|error| error.to_string())?;
        if definition_id.scope() == harness_contract::agent::DefinitionScope::Builtin {
            return Ok(None);
        }

        let relevant = self.update_agent_failure_window(&trigger)?;
        let failures = relevant
            .iter()
            .filter(|evaluation| !evaluation.succeeded)
            .collect::<Vec<_>>();
        if relevant.len() < 3 || failures.len() < 2 {
            return Ok(None);
        }
        let success_count = relevant.len().saturating_sub(failures.len());
        if success_count.saturating_mul(1_000) / relevant.len() >= 700 {
            return Ok(None);
        }

        let mut failure_patterns = failures
            .iter()
            .filter_map(|evaluation| evaluation.failure.as_deref())
            .map(|failure| failure.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|failure| !failure.is_empty())
            .collect::<Vec<_>>();
        failure_patterns.sort();
        failure_patterns.dedup();
        failure_patterns.truncate(8);
        let evidence_refs = failures
            .iter()
            .rev()
            .take(10)
            .map(|evaluation| {
                EvidenceRef::observed("agent_run_evaluation", evaluation.evaluation_id.clone())
                    .with_source(format!(
                        "{}@{}",
                        trigger.definition_id, trigger.definition_revision
                    ))
            })
            .collect::<Vec<_>>();
        let signal_seed = format!(
            "{}:{}:{}:{}",
            definition_id.as_str(),
            trigger.definition_revision,
            trigger.environment_fingerprint,
            source_event.event_id
        );
        let signal_digest = format!("{:x}", Sha256::digest(signal_seed.as_bytes()));
        Ok(Some(EvolutionSignal {
            signal_id: format!("evo-signal-agent-{}", &signal_digest[..20]),
            signal_type: EvolutionSignalType::AgentFailurePattern,
            source: EvolutionSignalSource {
                owner: format!("agent-definition:{}", definition_id.as_str()),
                session_id: ref_id(source_event, "session"),
                agent_id: Some(trigger.agent_instance_id.clone()),
                team_id: trigger.team_id.clone(),
                run_id: Some(trigger.run_id.clone()),
            },
            evidence_refs,
            severity: EvolutionSignalSeverity::Warning,
            summary: format!(
                "Agent definition {}@{} repeatedly failed in environment {}: {}",
                definition_id.as_str(),
                trigger.definition_revision,
                trigger.environment_fingerprint,
                if failure_patterns.is_empty() {
                    "no structured failure reason".to_string()
                } else {
                    failure_patterns.join("; ")
                }
            ),
            suggested_action:
                "Create an isolated definition revision and compare it against the bound baseline."
                    .to_string(),
            immediate_task_can_continue: true,
            scope: EvolutionSignalScope {
                workspace_identity: definition_id
                    .as_str()
                    .split('/')
                    .nth(1)
                    .unwrap_or("global")
                    .to_string(),
                affected_subject: definition_id.as_str().to_string(),
                workload_fingerprint: format!(
                    "{}:{}:{}",
                    trigger.task_domain, trigger.complexity, trigger.role_slot_id
                ),
                config_definition_revision: format!(
                    "{}@{}",
                    definition_id.as_str(),
                    trigger.definition_revision
                ),
                provider: trigger.provider.clone(),
                model: trigger.model.clone(),
                evaluation_environment: trigger.environment_fingerprint.clone(),
            },
            created_at_ms: u128::from(source_event.created_at_ms),
        }))
    }

    fn update_agent_failure_window(
        &self,
        trigger: &crate::AgentRunEvaluation,
    ) -> Result<Vec<AgentFailureObservation>, String> {
        let key_source = format!(
            "{}:{}:{}",
            trigger.definition_id, trigger.definition_revision, trigger.environment_fingerprint
        );
        let digest = format!("{:x}", Sha256::digest(key_source.as_bytes()));
        let checkpoint_id = format!("{AGENT_FAILURE_WINDOW_PREFIX}{}", &digest[..24]);
        for _ in 0..32 {
            let checkpoint = self
                .event_store
                .projection_checkpoint(&checkpoint_id)
                .map_err(|error| error.to_string())?;
            let mut window: AgentFailureWindow = checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.payload.get("window"))
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|error| error.to_string())?
                .unwrap_or_default();
            if window
                .observations
                .iter()
                .any(|observation| observation.evaluation_id == trigger.evaluation_id)
            {
                return Ok(window.observations);
            }
            window.observations.push(AgentFailureObservation {
                evaluation_id: trigger.evaluation_id.clone(),
                run_id: trigger.run_id.clone(),
                succeeded: trigger.is_success(),
                failure: trigger.failure.clone(),
                created_at_ms: trigger.created_at_ms,
            });
            window
                .observations
                .sort_by_key(|observation| observation.created_at_ms);
            if window.observations.len() > AGENT_FAILURE_WINDOW_SIZE {
                window
                    .observations
                    .drain(..window.observations.len() - AGENT_FAILURE_WINDOW_SIZE);
            }
            let expected_revision = checkpoint
                .as_ref()
                .map_or(0, |checkpoint| checkpoint.revision);
            match self.event_store.compare_and_put_projection_checkpoint(
                &checkpoint_id,
                checkpoint
                    .as_ref()
                    .map_or(1, |checkpoint| checkpoint.source_cursor.saturating_add(1)),
                expected_revision,
                &serde_json::json!({
                    "definition_id": trigger.definition_id,
                    "definition_revision": trigger.definition_revision,
                    "environment_fingerprint": trigger.environment_fingerprint,
                    "window": window,
                }),
                now_ms(),
            ) {
                Ok(_) => return Ok(window.observations),
                Err(crate::RuntimeEventStoreError::StaleRevision { .. })
                | Err(crate::RuntimeEventStoreError::TransactionConflict { .. }) => continue,
                Err(error) => return Err(error.to_string()),
            }
        }
        Err("agent failure window remained contended after bounded retries".to_string())
    }

    fn cursor(&self) -> Result<u64, String> {
        Ok(self
            .event_store
            .projection_checkpoint(PROJECTOR_ID)
            .map_err(|error| error.to_string())?
            .map(|checkpoint| checkpoint.source_cursor)
            .unwrap_or_default())
    }

    fn checkpoint(&self, source_cursor: u64) -> Result<(), String> {
        self.event_store
            .put_projection_checkpoint(
                PROJECTOR_ID,
                source_cursor,
                &serde_json::json!({
                    "projector": "evolution_signal",
                    "source_cursor": source_cursor,
                }),
                now_ms(),
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn dead_letter(
        &self,
        event: &DurableRuntimeEvent,
        source_cursor: u64,
        error: &str,
    ) -> Result<(), String> {
        let key = format!("dead-letter:{}", event.event_id);
        if self
            .event_store
            .event_by_idempotency_key(PROJECTOR_STREAM, &key)
            .map_err(|store_error| store_error.to_string())?
            .is_some()
        {
            return Ok(());
        }
        for _ in 0..32 {
            let mut index = self.failure_index()?;
            index.unresolved_count = index.unresolved_count.saturating_add(1);
            let frozen_page = if index.tail_source_event_ids.len() >= FAILURE_CATALOG_PAGE_SIZE {
                let page = ProjectorFailureCatalogPage {
                    page: index.tail_page,
                    source_event_ids: std::mem::take(&mut index.tail_source_event_ids),
                };
                index.tail_page = index.tail_page.saturating_add(1);
                Some(page)
            } else {
                None
            };
            index.tail_source_event_ids.push(event.event_id.clone());
            if index.unresolved_count == 1 {
                index.head_page = index.tail_page;
                index.head_offset = index.tail_source_event_ids.len().saturating_sub(1);
            }
            let expected_revision = self
                .event_store
                .stream_revision(PROJECTOR_STREAM)
                .map_err(|store_error| store_error.to_string())?;
            let mut expected_streams = vec![ExpectedStreamRevision {
                stream_id: PROJECTOR_STREAM.to_string(),
                expected_revision,
            }];
            let mut events = vec![
                projector_event(
                    FAILED_KIND,
                    Some("dead_letter"),
                    vec![RuntimeEventRef {
                        kind: "source_event".to_string(),
                        id: event.event_id.clone(),
                    }],
                    serde_json::json!({
                        "source_cursor": source_cursor,
                        "source_event_id": event.event_id,
                        "source_kind": event.kind,
                        "attempts": MAX_SOURCE_RETRIES,
                        "error": error,
                    }),
                    key.clone(),
                ),
                projector_event(
                    FAILURE_INDEX_KIND,
                    Some("indexed"),
                    Vec::new(),
                    serde_json::json!({"index": index}),
                    format!("failure-index-dead-letter:{}", event.event_id),
                ),
            ];
            if let Some(page) = frozen_page {
                let stream = failure_catalog_stream(page.page);
                expected_streams.push(ExpectedStreamRevision {
                    stream_id: stream.clone(),
                    expected_revision: 0,
                });
                events.push(RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: stream,
                        scope: RuntimeEventScope::Evolution,
                        kind: FAILURE_CATALOG_PAGE_KIND.to_string(),
                        status: Some("frozen".to_string()),
                        actor: Some("runtime.evolution_projector".to_string()),
                        refs: Vec::new(),
                        payload: serde_json::json!({"page": page}),
                    },
                    idempotency_key: Some(format!(
                        "failure-catalog-page:{}",
                        index.tail_page.saturating_sub(1)
                    )),
                    schema_version: 2,
                });
            }
            match self
                .event_store
                .append_transaction(AppendTransactionRequest {
                    transaction_id: format!(
                        "evolution-projector-dead-letter:{}:{expected_revision}",
                        event.event_id
                    ),
                    expected_streams,
                    events,
                }) {
                Ok(_) => return Ok(()),
                Err(crate::RuntimeEventStoreError::StaleRevision { .. }) => continue,
                Err(error) => return Err(error.to_string()),
            }
        }
        Err("projector failure index remained contended during dead letter".to_string())
    }
}

fn outcome_signal(event: &DurableRuntimeEvent) -> Result<Option<EvolutionSignal>, String> {
    let outcome = serde_json::from_value::<ExecutionOutcome>(event.payload.clone())
        .map_err(|error| format!("canonical outcome payload is invalid: {error}"))?;
    let classification = match &outcome.terminal {
        OutcomeTerminalClass::Failed(reason)
        | OutcomeTerminalClass::Blocked(reason)
        | OutcomeTerminalClass::PartialFailure(reason) => Some((
            EvolutionSignalType::RecoveryGap,
            EvolutionSignalSeverity::Critical,
            format!(
                "{} execution {}: {}",
                outcome.terminal.class_name(),
                outcome.identity.execution_id,
                reason
            ),
            "Create an isolated recovery candidate and falsify it against the pinned baseline.",
            false,
        )),
        OutcomeTerminalClass::Cancelled(reason) => Some((
            EvolutionSignalType::SlowProgress,
            EvolutionSignalSeverity::Info,
            format!(
                "execution {} was cancelled: {}",
                outcome.identity.execution_id, reason
            ),
            "Inspect the attributable cancellation evidence before proposing any policy change.",
            true,
        )),
        OutcomeTerminalClass::Succeeded(_) if outcome.usage.duplicate_tool_calls > 0 => Some((
            EvolutionSignalType::LowNoveltyToolLoop,
            EvolutionSignalSeverity::Warning,
            format!(
                "execution {} completed with {} duplicate tool calls",
                outcome.identity.execution_id, outcome.usage.duplicate_tool_calls
            ),
            "Compare a dependency-aware batch or Tool DAG candidate against the pinned baseline.",
            true,
        )),
        _ => None,
    };
    let Some((signal_type, severity, summary, suggested_action, continue_task)) = classification
    else {
        return Ok(None);
    };
    let mut evidence_refs = outcome.evidence_refs.clone();
    evidence_refs.push(
        EvidenceRef::observed("execution_outcome", event.event_id.clone())
            .with_source(outcome.identity.execution_id.clone()),
    );
    let signal_digest = format!("{:x}", Sha256::digest(event.event_id.as_bytes()));
    let provider = outcome.provider.as_ref();
    let affected_subject = outcome
        .identity
        .agent_id
        .as_deref()
        .or(outcome.identity.team_id.as_deref())
        .unwrap_or(&outcome.identity.session_id)
        .to_string();
    let scope = EvolutionSignalScope {
        workspace_identity: outcome.runtime.workspace_key.clone(),
        affected_subject,
        workload_fingerprint: outcome
            .strategy_feedback
            .workload
            .as_ref()
            .map(harness_contract::strategy::StrategyWorkloadFingerprint::digest)
            .unwrap_or_else(|| "unscoped".to_string()),
        config_definition_revision: outcome.runtime.config_revision.clone(),
        provider: provider
            .map(|provider| provider.provider_name.clone())
            .unwrap_or_else(|| "unknown".to_string()),
        model: provider
            .map(|provider| provider.model.clone())
            .unwrap_or_else(|| "unknown".to_string()),
        evaluation_environment: if outcome.strategy_feedback.evaluation_environment.is_empty() {
            "production".to_string()
        } else {
            outcome.strategy_feedback.evaluation_environment.clone()
        },
    };
    Ok(Some(EvolutionSignal {
        signal_id: format!("evo-signal-outcome-{}", &signal_digest[..24]),
        signal_type,
        source: EvolutionSignalSource {
            owner: "runtime.outcome".to_string(),
            session_id: Some(outcome.identity.session_id),
            agent_id: outcome.identity.agent_id,
            team_id: outcome.identity.team_id,
            run_id: Some(outcome.identity.execution_id),
        },
        evidence_refs,
        severity,
        summary,
        suggested_action: suggested_action.to_string(),
        immediate_task_can_continue: continue_task,
        scope,
        created_at_ms: u128::from(event.created_at_ms),
    }))
}

fn signal_from_source_event(event: &DurableRuntimeEvent) -> Option<EvolutionSignal> {
    let (signal_type, severity, summary, suggested_action, continue_task) =
        classify_source_event(event)?;
    let source = EvolutionSignalSource {
        owner: event
            .actor
            .clone()
            .unwrap_or_else(|| scope_owner(event.scope).to_string()),
        session_id: ref_id(event, "session"),
        agent_id: ref_id(event, "agent_run").or_else(|| ref_id(event, "agent")),
        team_id: ref_id(event, "team_run").or_else(|| ref_id(event, "team")),
        run_id: ref_id(event, "run"),
    };
    let digest = format!("{:x}", Sha256::digest(event.event_id.as_bytes()));
    Some(EvolutionSignal {
        signal_id: format!("evo-signal-source-{}", &digest[..24]),
        signal_type,
        source,
        evidence_refs: vec![
            EvidenceRef::observed("runtime_event", event.event_id.clone())
                .with_source(event.kind.clone()),
        ],
        severity,
        summary,
        suggested_action,
        immediate_task_can_continue: continue_task,
        scope: EvolutionSignalScope {
            workspace_identity: ref_id(event, "workspace").unwrap_or_else(|| "global".to_string()),
            affected_subject: event.stream_id.clone(),
            workload_fingerprint: "unscoped".to_string(),
            config_definition_revision: format!("event-schema-v{}", event.schema_version),
            provider: "unknown".to_string(),
            model: "unknown".to_string(),
            evaluation_environment: "production".to_string(),
        },
        created_at_ms: u128::from(event.created_at_ms),
    })
}

fn classify_source_event(
    event: &DurableRuntimeEvent,
) -> Option<(
    EvolutionSignalType,
    EvolutionSignalSeverity,
    String,
    String,
    bool,
)> {
    match event.kind.as_str() {
        "goal.intervention" => {
            let intervention = event.payload.get("intervention");
            let reason = intervention
                .and_then(|value| value.get("reason"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("Runtime changed execution strategy after a progress review");
            let low_novelty = reason.to_ascii_lowercase().contains("novel")
                || reason.to_ascii_lowercase().contains("repeat");
            Some((
                if low_novelty {
                    EvolutionSignalType::LowNoveltyToolLoop
                } else {
                    EvolutionSignalType::SlowProgress
                },
                EvolutionSignalSeverity::Warning,
                reason.to_string(),
                "Compare the triggering observations and execution strategy in a bounded paired scenario."
                    .to_string(),
                true,
            ))
        }
        "goal.observation" => {
            let observation = event.payload.get("observation")?;
            let failed = observation
                .get("result_class")
                .and_then(serde_json::Value::as_str)
                == Some("failed")
                || observation
                    .get("failure_class")
                    .is_some_and(|value| !value.is_null());
            let tool_calls = observation
                .pointer("/cost_delta/tool_calls")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();
            let information_gain = observation
                .pointer("/information_gain/new_evidence")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default()
                + observation
                    .pointer("/information_gain/resolved_unknowns")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
            if failed {
                Some((
                    EvolutionSignalType::AgentFailurePattern,
                    EvolutionSignalSeverity::Warning,
                    "A typed Goal observation recorded an execution failure.".to_string(),
                    "Cluster attributable failures and run a frozen recovery scenario.".to_string(),
                    true,
                ))
            } else if tool_calls > 0 && information_gain == 0 {
                Some((
                    EvolutionSignalType::LowNoveltyToolLoop,
                    EvolutionSignalSeverity::Warning,
                    "Tool work consumed budget without verified information gain.".to_string(),
                    "Compare batched evidence acquisition or a different execution strategy."
                        .to_string(),
                    true,
                ))
            } else {
                None
            }
        }
        "goal.completed" if event.status.as_deref() != Some("satisfied") => Some((
            EvolutionSignalType::RecoveryGap,
            EvolutionSignalSeverity::Warning,
            format!(
                "A Goal completed without satisfaction ({})",
                event.status.as_deref().unwrap_or("unknown")
            ),
            "Preserve the terminal evidence and evaluate a bounded recovery strategy.".to_string(),
            false,
        )),
        _ => None,
    }
}

fn ref_id(event: &DurableRuntimeEvent, kind: &str) -> Option<String> {
    event
        .refs
        .iter()
        .find(|reference| reference.kind == kind)
        .map(|reference| reference.id.clone())
}

const fn scope_owner(scope: RuntimeEventScope) -> &'static str {
    match scope {
        RuntimeEventScope::Agent => "runtime.agent",
        RuntimeEventScope::Team => "runtime.team",
        RuntimeEventScope::Task => "runtime.task",
        RuntimeEventScope::Goal => "runtime.goal",
        RuntimeEventScope::Session => "runtime.session",
        RuntimeEventScope::Skill => "runtime.skill",
        RuntimeEventScope::Tool => "runtime.tool",
        RuntimeEventScope::Knowledge => "runtime.knowledge",
        _ => "runtime",
    }
}

fn is_projector_output_only(batch: &CommittedEventBatch) -> bool {
    !batch.events.is_empty()
        && batch.events.iter().all(|event| {
            event.scope == RuntimeEventScope::Evolution
                && (event.stream_id.starts_with("evolution:")
                    || event.stream_id == PROJECTOR_STREAM)
        })
}

fn failure_catalog_stream(page: u64) -> String {
    format!("evolution:projector-failure-catalog:v2:{page:020}")
}

fn projector_event(
    kind: &str,
    status: Option<&str>,
    refs: Vec<RuntimeEventRef>,
    payload: serde_json::Value,
    idempotency_key: String,
) -> RuntimeTransactionEventInput {
    RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: PROJECTOR_STREAM.to_string(),
            scope: RuntimeEventScope::Evolution,
            kind: kind.to_string(),
            status: status.map(str::to_string),
            actor: Some("runtime.evolution_signal_projector".to_string()),
            refs,
            payload,
        },
        idempotency_key: Some(idempotency_key),
        schema_version: 1,
    }
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
            OutcomeIdentity, OutcomeObservation, OutcomeQuality, OutcomeTiming, OutcomeUsage,
            RuntimeIdentity, StrategyIdentity, OUTCOME_SCHEMA_REVISION,
        },
        reality::EvidenceCompleteness,
        strategy::ExecutionCandidateKind,
    };

    fn failed_agent_evaluation(index: u64) -> crate::AgentRunEvaluation {
        crate::AgentRunEvaluation {
            evaluation_id: format!("evaluation-{index}"),
            run_id: format!("run-{index}"),
            agent_instance_id: format!("agent-{index}"),
            definition_id: "workspace/cowd/learner".to_string(),
            definition_revision: 1,
            binding_digest: "binding-digest".to_string(),
            release_assignment_id: None,
            release_generation: None,
            release_channel: Some(harness_contract::agent::ReleaseChannel::Stable),
            task_id: format!("task-{index}"),
            task_domain: "coding".to_string(),
            complexity: "medium".to_string(),
            role_slot_id: "worker".to_string(),
            model: "test-model".to_string(),
            provider: "test-provider".to_string(),
            granted_capabilities: vec!["read".to_string()],
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            memory_reality_fingerprint: "memory-fingerprint".to_string(),
            team_id: None,
            environment_fingerprint: "environment-fingerprint".to_string(),
            terminal_status: harness_contract::agent::AgentTerminalStatus::Failed,
            acceptance: vec!["task_success".to_string()],
            outcome: String::new(),
            failure: Some("repeated evidence validation failure".to_string()),
            input_tokens: 10,
            output_tokens: 2,
            tool_calls: 1,
            evidence_refs: vec![format!("evidence-{index}")],
            created_at_ms: index,
        }
    }

    fn failed_outcome() -> ExecutionOutcome {
        ExecutionOutcome {
            identity: OutcomeIdentity {
                execution_id: "execution-outcome-failed".to_string(),
                session_id: "session-1".to_string(),
                turn_id: "turn-1".to_string(),
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
                config_revision: "config".to_string(),
            },
            provider: None,
            strategy: StrategyIdentity {
                decision_id: "decision".to_string(),
                policy_revision: "policy".to_string(),
                decision_source: "test".to_string(),
                selected_candidate: ExecutionCandidateKind::Direct,
                selected_pattern: "direct".to_string(),
            },
            timing: OutcomeTiming {
                started_at_ms: 1,
                completed_at_ms: 2,
                duration_ms: 1,
            },
            usage: OutcomeUsage::default(),
            terminal: OutcomeTerminalClass::Failed("provider failed".to_string()),
            quality: OutcomeQuality::Unknown,
            observation: OutcomeObservation {
                source: "test".to_string(),
                observed_at_ms: 2,
                freshness_ms: 0,
            },
            strategy_feedback: Default::default(),
            evidence_refs: Vec::new(),
            evidence_completeness: EvidenceCompleteness::None,
            schema_revision: OUTCOME_SCHEMA_REVISION,
        }
    }

    fn append_foreground_probe(
        store: &RuntimeEventStore,
        prefix: &str,
        samples: usize,
    ) -> (Duration, Vec<u128>) {
        let started = Instant::now();
        let mut latencies = Vec::with_capacity(samples);
        for index in 0..samples {
            let one = Instant::now();
            store
                .append(RuntimeEventInput {
                    stream_id: format!("{prefix}:{index}"),
                    scope: RuntimeEventScope::Session,
                    kind: "session.observed".to_string(),
                    status: None,
                    actor: Some("foreground-probe".to_string()),
                    refs: Vec::new(),
                    payload: serde_json::json!({"index": index}),
                })
                .expect("foreground append");
            latencies.push(one.elapsed().as_micros());
        }
        (started.elapsed(), latencies)
    }

    fn latency_percentile(mut samples: Vec<u128>, percentile: usize) -> u128 {
        samples.sort_unstable();
        samples[(samples.len().saturating_sub(1) * percentile) / 100]
    }

    #[test]
    fn ten_minutes_of_idle_projection_passes_create_zero_commits() {
        const TEN_MINUTES_AT_ONE_SECOND_POLL: usize = 600;
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let evolution = EvolutionSignalProjector::new(Arc::clone(&events), discovery);
        let skill = crate::SkillMaintenanceProjector::new(Arc::clone(&events));
        let initial_cursor = events.current_commit_cursor();

        for _ in 0..TEN_MINUTES_AT_ONE_SECOND_POLL {
            assert_eq!(evolution.run_once(PROJECTOR_WORKER_BATCH).unwrap(), 0);
            assert_eq!(skill.project_available(PROJECTOR_WORKER_BATCH).unwrap(), 0);
        }

        assert_eq!(events.current_commit_cursor(), initial_cursor);
    }

    #[derive(Default)]
    struct ProbeAggregate {
        rounds: u128,
        elapsed_us: u128,
        latencies_us: Vec<u128>,
    }

    impl ProbeAggregate {
        fn record(&mut self, observation: (Duration, Vec<u128>)) {
            self.rounds = self.rounds.saturating_add(1);
            self.elapsed_us = self.elapsed_us.saturating_add(observation.0.as_micros());
            self.latencies_us.extend(observation.1);
        }

        fn elapsed_mean_us(&self) -> u128 {
            self.elapsed_us / self.rounds.max(1)
        }

        fn p95_us(&self) -> u128 {
            latency_percentile(self.latencies_us.clone(), 95)
        }

        fn p99_us(&self) -> u128 {
            latency_percentile(self.latencies_us.clone(), 99)
        }
    }

    fn assert_regression_within(label: &str, baseline: u128, projected: u128, limit_percent: u128) {
        assert!(
            projected.saturating_mul(100)
                <= baseline.saturating_mul(100_u128.saturating_add(limit_percent)),
            "{label} regression exceeded {limit_percent}%: baseline={baseline}, projected={projected}"
        );
    }

    fn baseline_probe(backlog: usize, samples: usize, prefix: &str) -> (Duration, Vec<u128>) {
        let root = tempfile::tempdir().unwrap();
        let events = RuntimeEventStore::open(root.path().join("runtime.sqlite")).unwrap();
        if backlog > 0 {
            append_foreground_probe(&events, &format!("{prefix}-backlog"), backlog);
        }
        append_foreground_probe(&events, prefix, samples)
    }

    async fn projected_probe(
        backlog: usize,
        samples: usize,
        prefix: &str,
    ) -> (Duration, Vec<u128>) {
        let root = tempfile::tempdir().unwrap();
        let events = Arc::new(RuntimeEventStore::open(root.path().join("runtime.sqlite")).unwrap());
        if backlog > 0 {
            append_foreground_probe(&events, &format!("{prefix}-backlog"), backlog);
        }
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = Arc::new(EvolutionSignalProjector::new(
            Arc::clone(&events),
            discovery,
        ));
        projector.start();
        let observation = append_foreground_probe(&events, prefix, samples);
        if backlog > 0 {
            let catchup_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            let catchup_health = loop {
                if let Ok(health) = projector.health() {
                    if health.source_cursor > 0 {
                        break health;
                    }
                }
                assert!(
                    tokio::time::Instant::now() < catchup_deadline,
                    "projector did not begin durable catch-up"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            assert!(
                catchup_health.lag_commits > 0,
                "probe requires a real remaining backlog"
            );
        }
        projector.shutdown().await;
        observation
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "explicit paired V649 performance evidence"]
    async fn paired_foreground_probe_with_and_without_projector_is_bounded() {
        const ROUNDS: usize = 10;
        const SAMPLES: usize = 2_000;
        let mut baseline = ProbeAggregate::default();
        let mut projected = ProbeAggregate::default();
        for round in 0..ROUNDS {
            if round % 2 == 0 {
                baseline.record(baseline_probe(0, SAMPLES, &format!("probe-{round}")));
                projected.record(projected_probe(0, SAMPLES, &format!("probe-{round}")).await);
            } else {
                projected.record(projected_probe(0, SAMPLES, &format!("probe-{round}")).await);
                baseline.record(baseline_probe(0, SAMPLES, &format!("probe-{round}")));
            }
        }
        eprintln!(
            "V649 paired foreground probe rounds={ROUNDS} samples={SAMPLES} baseline_mean_us={} projected_mean_us={} baseline_p95_us={} projected_p95_us={} baseline_p99_us={} projected_p99_us={}",
            baseline.elapsed_mean_us(),
            projected.elapsed_mean_us(),
            baseline.p95_us(),
            projected.p95_us(),
            baseline.p99_us(),
            projected.p99_us(),
        );
        assert_regression_within(
            "foreground throughput",
            baseline.elapsed_mean_us(),
            projected.elapsed_mean_us(),
            2,
        );
        assert_regression_within("foreground p95", baseline.p95_us(), projected.p95_us(), 3);
        assert_regression_within("foreground p99", baseline.p99_us(), projected.p99_us(), 5);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "explicit active-catchup V651 performance evidence"]
    async fn paired_foreground_probe_during_projector_catchup_is_bounded() {
        const ROUNDS: usize = 10;
        const BACKLOG: usize = 512;
        const SAMPLES: usize = 10_000;
        let mut baseline = ProbeAggregate::default();
        let mut projected = ProbeAggregate::default();
        for round in 0..ROUNDS {
            if round % 2 == 0 {
                baseline.record(baseline_probe(BACKLOG, SAMPLES, &format!("active-{round}")));
                projected
                    .record(projected_probe(BACKLOG, SAMPLES, &format!("active-{round}")).await);
            } else {
                projected
                    .record(projected_probe(BACKLOG, SAMPLES, &format!("active-{round}")).await);
                baseline.record(baseline_probe(BACKLOG, SAMPLES, &format!("active-{round}")));
            }
        }
        eprintln!(
            "V651 active catchup probe rounds={ROUNDS} backlog={BACKLOG} samples={SAMPLES} baseline_mean_us={} projected_mean_us={} baseline_p95_us={} projected_p95_us={} baseline_p99_us={} projected_p99_us={}",
            baseline.elapsed_mean_us(),
            projected.elapsed_mean_us(),
            baseline.p95_us(),
            projected.p95_us(),
            baseline.p99_us(),
            projected.p99_us(),
        );
        assert_regression_within(
            "active-catchup foreground throughput",
            baseline.elapsed_mean_us(),
            projected.elapsed_mean_us(),
            2,
        );
        assert_regression_within(
            "active-catchup foreground p95",
            baseline.p95_us(),
            projected.p95_us(),
            3,
        );
        assert_regression_within(
            "active-catchup foreground p99",
            baseline.p99_us(),
            projected.p99_us(),
            5,
        );
    }

    #[test]
    fn projector_replays_source_once_and_crosses_its_own_checkpoint() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = EvolutionSignalProjector::new(Arc::clone(&events), Arc::clone(&discovery));
        events
            .append(RuntimeEventInput {
                stream_id: "goal:goal-1".to_string(),
                scope: RuntimeEventScope::Goal,
                kind: "goal.intervention".to_string(),
                status: Some("replan".to_string()),
                actor: Some("runtime.intervention_policy".to_string()),
                refs: vec![RuntimeEventRef {
                    kind: "session".to_string(),
                    id: "session-1".to_string(),
                }],
                payload: serde_json::json!({
                    "intervention": {
                        "reason": "repeated low novelty reads",
                    }
                }),
            })
            .expect("source event");
        assert_eq!(projector.run_once(1).expect("first pass"), 1);
        assert_eq!(discovery.list_signals().expect("signals").len(), 1);
        assert_eq!(discovery.list_cases(25).expect("cases").len(), 1);
        assert!(discovery.list_diagnoses().expect("diagnoses").is_empty());
        assert!(discovery.list_missions().expect("missions").is_empty());
        assert!(discovery.list_proposals().expect("proposals").is_empty());
        assert_eq!(projector.run_once(64).expect("cross outputs"), 0);
        let health = projector.health().expect("health after checkpoint");
        assert_eq!(health.source_cursor, health.latest_commit_cursor);
        assert_eq!(health.lag_commits, 0);

        events
            .append(RuntimeEventInput {
                stream_id: "goal:goal-2".to_string(),
                scope: RuntimeEventScope::Goal,
                kind: "goal.observation".to_string(),
                status: Some("failed".to_string()),
                actor: Some("runtime.goal".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "observation": {
                        "result_class": "failed",
                        "failure_class": "provider_unavailable",
                        "cost_delta": {"tool_calls": 0},
                        "information_gain": {
                            "new_evidence": 0,
                            "resolved_unknowns": 0
                        }
                    }
                }),
            })
            .expect("second source event");
        assert_eq!(projector.run_once(1).expect("second pass"), 1);
        assert_eq!(discovery.list_signals().expect("signals").len(), 2);
        assert!(discovery.list_proposals().expect("proposals").is_empty());

        for index in 1..=3 {
            events
                .append(RuntimeEventInput {
                    stream_id: format!("agent-evaluation:run-{index}"),
                    scope: RuntimeEventScope::Evolution,
                    kind: "agent.run_evaluated".to_string(),
                    status: Some("failed".to_string()),
                    actor: Some("runtime.agent_evaluation".to_string()),
                    refs: vec![RuntimeEventRef {
                        kind: "run".to_string(),
                        id: format!("run-{index}"),
                    }],
                    payload: serde_json::json!({
                        "evaluation": failed_agent_evaluation(index),
                    }),
                })
                .expect("evaluation source event");
        }
        let mut projected_evaluations = 0;
        for _ in 0..16 {
            projected_evaluations += projector.run_once(3).expect("evaluation pass");
            if projected_evaluations == 3 {
                break;
            }
        }
        assert_eq!(projected_evaluations, 3);
        assert_eq!(discovery.list_signals().expect("signals").len(), 3);
        assert!(discovery.list_proposals().expect("proposals").is_empty());

        let restarted = EvolutionSignalProjector::new(events, Arc::clone(&discovery));
        assert_eq!(restarted.run_once(64).expect("restart pass"), 0);
        assert_eq!(restarted.health().expect("restart health").lag_commits, 0);
        assert_eq!(discovery.list_signals().expect("signals").len(), 3);
        assert!(discovery.list_proposals().expect("proposals").is_empty());
    }

    #[test]
    fn projector_checkpoint_is_mutable_and_does_not_emit_a_commit() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = EvolutionSignalProjector::new(Arc::clone(&events), discovery);
        events
            .append(RuntimeEventInput {
                stream_id: "unrelated".to_string(),
                scope: RuntimeEventScope::Session,
                kind: "session.observed".to_string(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .expect("source event");
        let commits = events.subscribe_commits();
        let commit_cursor = *commits.borrow();

        assert_eq!(projector.run_once(64).expect("projector pass"), 1);
        assert_eq!(*commits.borrow(), commit_cursor);
        assert_eq!(
            events
                .projection_checkpoint(PROJECTOR_ID)
                .expect("checkpoint")
                .expect("stored checkpoint")
                .source_cursor,
            commit_cursor
        );
        assert!(events
            .list_stream(PROJECTOR_STREAM)
            .expect("projector stream")
            .is_empty());
        assert_eq!(projector.run_once(64).expect("idempotent pass"), 0);
    }

    #[test]
    fn legacy_signal_bootstrap_is_bounded_replayable_and_does_not_duplicate_case() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = EvolutionSignalProjector::new(Arc::clone(&events), Arc::clone(&discovery));
        let mut signal = EvolutionSignal::low_novelty_tool_loop(
            "legacy-runtime",
            "legacy-session",
            vec![EvidenceRef::observed("legacy", "signal-evidence")],
        );
        signal.signal_id = "legacy-signal-1".to_string();
        events
            .append(RuntimeEventInput {
                stream_id: format!("evolution:signal:{}", signal.signal_id),
                scope: RuntimeEventScope::Evolution,
                kind: "evolution.signal.recorded.v1".to_string(),
                status: Some("warning".to_string()),
                actor: Some("legacy.runtime".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"signal": signal}),
            })
            .expect("legacy signal");

        assert_eq!(projector.run_once(64).expect("bootstrap"), 0);
        assert_eq!(discovery.list_cases(25).expect("cases").len(), 1);
        assert!(events
            .projection_checkpoint(LEGACY_BOOTSTRAP_ID)
            .expect("checkpoint")
            .is_some());
        assert_eq!(projector.run_once(64).expect("idempotent restart"), 0);
        assert_eq!(discovery.case_index().expect("case index").total_cases, 1);
    }

    #[test]
    fn canonical_outcome_enters_governed_evolution_without_direct_promotion() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = EvolutionSignalProjector::new(Arc::clone(&events), Arc::clone(&discovery));
        crate::execution_core::OutcomeService::new(Arc::clone(&events))
            .record_terminal(&failed_outcome())
            .expect("outcome");

        assert_eq!(projector.run_once(64).expect("project"), 1);
        assert_eq!(discovery.list_signals().expect("signals").len(), 1);
        assert!(discovery.list_proposals().expect("proposals").is_empty());
        let cases = discovery.list_cases(25).expect("cases");
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].state, crate::EvolutionCaseState::Ready);
        assert!(events
            .list_scope(RuntimeEventScope::Evolution, 1_000)
            .expect("events")
            .iter()
            .all(|event| !event.kind.contains("stable")));
    }

    #[test]
    fn projector_repairs_historical_dead_letter_after_checkpoint_advanced() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = EvolutionSignalProjector::new(Arc::clone(&events), Arc::clone(&discovery));
        let source = events
            .append(RuntimeEventInput {
                stream_id: "goal:repair-source".to_string(),
                scope: RuntimeEventScope::Goal,
                kind: "goal.intervention".to_string(),
                status: Some("replan".to_string()),
                actor: Some("runtime.intervention_policy".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "intervention": {"reason": "repeated low novelty reads"}
                }),
            })
            .expect("source event");
        let source_event = source.clone();
        projector
            .dead_letter(
                &source_event,
                source_event.commit_cursor,
                "historical projection failure",
            )
            .expect("historical dead letter");
        projector
            .checkpoint(source_event.commit_cursor)
            .expect("checkpoint already crossed source");
        assert_eq!(
            projector.health().expect("failed health").dead_letter_count,
            1
        );

        assert_eq!(projector.run_once(64).expect("repair pass"), 0);
        assert_eq!(
            projector
                .health()
                .expect("recovered health")
                .dead_letter_count,
            0
        );
        assert_eq!(discovery.list_signals().expect("signals").len(), 1);
        assert_eq!(discovery.list_cases(25).expect("cases").len(), 1);
        assert!(discovery.list_diagnoses().expect("diagnoses").is_empty());
        assert!(discovery.list_missions().expect("missions").is_empty());
        assert!(discovery.list_proposals().expect("proposals").is_empty());
        assert_eq!(
            events
                .replay_scope_kind(RuntimeEventScope::Evolution, RECOVERED_KIND)
                .expect("recovery evidence")
                .len(),
            1
        );

        assert_eq!(projector.run_once(64).expect("idempotent repair"), 0);
        assert_eq!(
            events
                .replay_scope_kind(RuntimeEventScope::Evolution, RECOVERED_KIND)
                .expect("single recovery evidence")
                .len(),
            1
        );
    }
}
