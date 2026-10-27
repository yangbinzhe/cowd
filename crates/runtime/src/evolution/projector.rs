//! Supervised projection from execution evidence to evolution signals.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

use harness_contract::outcome::{ExecutionOutcome, OutcomeTerminalClass};
use harness_contract::reality::EvidenceRef;
use sha2::{Digest, Sha256};

use super::{
    EvolutionDiscoveryService, EvolutionSignal, EvolutionSignalScope, EvolutionSignalSeverity,
    EvolutionSignalSource, EvolutionSignalType,
};
use crate::execution_core::outcome_service::OUTCOME_EVENT_KIND;
use crate::{
    AppendTransactionRequest, DurableRuntimeEvent, ExpectedStreamRevision, RuntimeEventInput,
    RuntimeEventRef, RuntimeEventScope, RuntimeEventStore, RuntimeProjectionDescriptor,
    RuntimeProjectionEventInterest, RuntimeProjectionInterest, RuntimeProjectionLane,
    RuntimeProjectionLatencyClass, RuntimeProjectionPass, RuntimeTransactionEventInput,
};

const PROJECTOR_STREAM: &str = "evolution-signal-projector";
pub(crate) const PROJECTOR_ID: &str = "projector:evolution-case:v2";
const LEGACY_BOOTSTRAP_ID: &str = "projector:evolution-case-bootstrap:v2";
const POLICY_BOUNDARY_ID: &str = "projector:evolution-case-policy-boundary:v1";
const PROJECTOR_BATCH: usize = 128;
// Evolution is explicitly lower priority than foreground execution. Keep each
// supervised pass small and leave a scheduling window between non-empty
// passes; the durable cursor provides eventual catch-up without turning an
// evidence burst into SQLite lock pressure on the active mission.
// Keep the maintenance worker's batch aligned with the independently bounded
// source scan.  A smaller worker batch checkpointed empty/non-matching
// commits repeatedly, creating avoidable SQLite writer contention under a
// foreground burst.  This does not expand the event/byte scan ceiling.
const PROJECTOR_WORKER_BATCH: usize = PROJECTOR_BATCH;
const MAX_SOURCE_RETRIES: usize = 3;
const REPAIR_BATCH: usize = 32;
// One Runtime transaction is already hard-capped at these exact limits by the
// canonical EventStore. Reading one commit at a time therefore makes the
// projector's peak source slice independently bounded by the same contract.
const MAX_SCAN_EVENTS: usize = 10_000;
const MAX_SCAN_BYTES: usize = 32 * 1024 * 1024;
const MAX_SCAN_WALL: Duration = Duration::from_millis(50);
// In-process commits wake the reactor immediately. This is only the
// cross-process/restart safety scan, so a one-second poll needlessly keeps the
// durable store hot while idle. Five seconds preserves bounded reconciliation
// without multiplying idle PostgreSQL reads across projection lanes.
const PROJECTOR_IDLE_POLL: Duration = Duration::from_secs(5);
const FAILED_KIND: &str = "evolution.signal.projector.failed.v1";
const RECOVERED_KIND: &str = "evolution.signal.projector.recovered.v1";
const FAILURE_INDEX_KIND: &str = "evolution.signal.projector.failure_index.v2";
const FAILURE_CATALOG_PAGE_KIND: &str = "evolution.signal.projector.failure_page.frozen.v2";
const FAILURE_CATALOG_PAGE_SIZE: usize = 1_024;
const AGENT_FAILURE_WINDOW_PREFIX: &str = "projector:evolution-agent-failure-window:v2:";
const AGENT_FAILURE_WINDOW_SIZE: usize = 128;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EvolutionAdmittedSourcePage {
    source_upper_cursor: u64,
    legacy_upper_position: Option<(u64, u32)>,
    max_commits: usize,
}

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
    workspace_identity: Option<String>,
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
            workspace_identity: None,
        }
    }

    pub(crate) fn with_workspace_identity(mut self, identity: String) -> Self {
        self.workspace_identity = (!identity.trim().is_empty()).then_some(identity);
        self
    }

    pub(crate) fn projection_lane(self: &Arc<Self>) -> Result<RuntimeProjectionLane, String> {
        let projector = Arc::clone(self);
        Ok(RuntimeProjectionLane::blocking(
            RuntimeProjectionDescriptor::new(
                PROJECTOR_ID,
                projection_interest(),
                PROJECTOR_WORKER_BATCH,
                PROJECTOR_IDLE_POLL,
            )?
            .with_latency_class(RuntimeProjectionLatencyClass::Maintenance),
            move |batch_size| {
                let before = projector.cursor()?;
                let processed = projector.run_once(batch_size)?;
                let after = projector.cursor()?;
                let scanned = usize::try_from(after.saturating_sub(before)).unwrap_or(usize::MAX);
                Ok(RuntimeProjectionPass::scanned(scanned, batch_size).with_matches(processed))
            },
        ))
    }

    pub(crate) fn ensure_source_epoch(&self) -> Result<(), String> {
        if let Some(entry) = self
            .event_store
            .projection_checkpoint(POLICY_BOUNDARY_ID)
            .map_err(|error| error.to_string())?
        {
            entry
                .payload
                .get("paused")
                .and_then(serde_json::Value::as_bool)
                .ok_or("invalid evolution source policy boundary")?;
            if let Some(legacy) = entry.payload.get("legacy_admission_unknown") {
                legacy
                    .as_bool()
                    .ok_or("invalid evolution legacy admission state")?;
            }
            return Ok(());
        }
        // Legacy cursors never recorded page admission. Do not fabricate that
        // missing evidence when a cold-off instance first acquires controls.
        let mut legacy = self.discovery.case_index()?.total_cases != 0;
        for id in [
            PROJECTOR_ID,
            LEGACY_BOOTSTRAP_ID,
            crate::evolution::collaboration_experience::PROJECTOR_ID,
        ] {
            legacy |= self
                .event_store
                .projection_checkpoint(id)
                .map_err(|error| error.to_string())?
                .is_some();
        }
        legacy |= !self
            .event_store
            .list_scope_kind_page_asc(
                RuntimeEventScope::Evolution,
                crate::evolution::collaboration_experience::EPISODE_KIND,
                None,
                1,
            )
            .map_err(|error| error.to_string())?
            .is_empty();
        match self.event_store.compare_and_put_projection_checkpoint(
            POLICY_BOUNDARY_ID,
            0,
            0,
            &serde_json::json!({"paused":false,"legacy_admission_unknown":legacy}),
            now_ms(),
        ) {
            Ok(_) => Ok(()),
            Err(crate::RuntimeEventStoreError::StaleRevision { .. }) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }

    // This is cursor bookkeeping in the original projection store, not a
    // second desired-policy authority. Configuration owns desired on/off.
    // Only the retained worker's physical drain owner may call this boundary.
    pub(crate) fn pause_sources(&self, physical_error: Option<&str>) -> Result<(), String> {
        self.ensure_source_epoch()?;
        let mut previous = self
            .event_store
            .projection_checkpoint(POLICY_BOUNDARY_ID)
            .map_err(|error| error.to_string())?;
        let admitted = previous
            .as_ref()
            .and_then(|entry| entry.payload.get("pending_page"))
            .filter(|value| !value.is_null())
            .cloned();
        let recovered = admitted.is_some();
        if let Some(admitted) = admitted {
            let plan: EvolutionAdmittedSourcePage = serde_json::from_value(admitted)
                .map_err(|error| format!("invalid evolution admitted source page: {error}"))?;
            self.run_once(plan.max_commits)?;
            previous = self
                .event_store
                .projection_checkpoint(POLICY_BOUNDARY_ID)
                .map_err(|error| error.to_string())?;
            if previous
                .as_ref()
                .and_then(|entry| entry.payload.get("pending_page"))
                .is_some_and(|value| !value.is_null())
            {
                return Err("evolution original admitted source page remains pending".into());
            }
        }
        let physical_error = if recovered { None } else { physical_error };
        let legacy_unknown = previous
            .as_ref()
            .is_some_and(|entry| entry.payload["legacy_admission_unknown"] == true);
        let physical_error = if legacy_unknown {
            Some("legacy evolution source admission is unproven")
        } else {
            physical_error
        };
        if previous
            .as_ref()
            .map(|entry| {
                entry
                    .payload
                    .get("paused")
                    .and_then(serde_json::Value::as_bool)
                    .ok_or("invalid evolution pause boundary")
            })
            .transpose()?
            .unwrap_or(false)
        {
            if physical_error.is_none() {
                return Ok(());
            }
        }
        let cut = self
            .event_store
            .durable_commit_cursor()
            .map_err(|error| error.to_string())?;
        self.event_store
            .put_projection_checkpoint_retrying(
                POLICY_BOUNDARY_ID,
                cut,
                &serde_json::json!({"paused": true, "physical_error": physical_error}),
                now_ms(),
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub(crate) fn reconcile_paused_claims(&self) -> Result<(), String> {
        self.repair_dead_letters(REPAIR_BATCH)?;
        let checkpoint = self
            .event_store
            .projection_checkpoint(POLICY_BOUNDARY_ID)
            .map_err(|error| error.to_string())?
            .ok_or("evolution pause boundary is missing")?;
        if checkpoint.payload["paused"].as_bool() != Some(true) {
            return Err("evolution claim drain requires a paused source".into());
        }
        let scan = match checkpoint.payload.get("analysis_drain") {
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|error| format!("invalid evolution claim continuation: {error}"))?,
            None => crate::evolution::analyst::EvolutionAnalysisDrainScan::default(),
        };
        let scan = crate::evolution::analyst::EvolutionAnalystService::scan_admitted_claims(
            &self.event_store,
            scan,
        )?;
        let after = checkpoint
            .payload
            .get("direct_signal_after")
            .filter(|value| !value.is_null())
            .map(|value| {
                serde_json::from_value::<(u64, u32)>(value.clone())
                    .map_err(|error| error.to_string())
            })
            .transpose()?;
        let (direct_after, direct_complete) =
            self.discovery.recover_admitted_signals_page(after)?;
        let mut payload = checkpoint.payload.clone();
        payload["direct_signal_after"] =
            serde_json::to_value(direct_after).map_err(|error| error.to_string())?;
        payload["direct_signal_scan_complete"] = serde_json::json!(direct_complete);
        payload["analysis_drain"] =
            serde_json::to_value(scan).map_err(|error| error.to_string())?;
        self.event_store
            .compare_and_put_projection_checkpoint(
                POLICY_BOUNDARY_ID,
                checkpoint.source_cursor,
                checkpoint.revision,
                &payload,
                now_ms(),
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub(crate) fn paused_claim_status(&self) -> Result<(bool, usize), String> {
        let checkpoint = self
            .event_store
            .projection_checkpoint(POLICY_BOUNDARY_ID)
            .map_err(|error| error.to_string())?
            .ok_or("evolution pause boundary is missing")?;
        if checkpoint.payload["paused"].as_bool() != Some(true) {
            return Err("evolution pause boundary is not paused".into());
        }
        let scan: crate::evolution::analyst::EvolutionAnalysisDrainScan = serde_json::from_value(
            checkpoint
                .payload
                .get("analysis_drain")
                .ok_or("evolution durable claim inspection has not completed")?
                .clone(),
        )
        .map_err(|error| format!("invalid evolution claim continuation: {error}"))?;
        if let Some(error) = checkpoint
            .payload
            .get("physical_error")
            .filter(|error| !error.is_null())
        {
            return Err(format!(
                "evolution original pass remains unsettled: {error}"
            ));
        }
        let dead_letters = usize::try_from(self.failure_index()?.unresolved_count)
            .map_err(|_| "evolution pending count overflow")?;
        let direct_complete = checkpoint
            .payload
            .get("direct_signal_scan_complete")
            .and_then(serde_json::Value::as_bool)
            .ok_or("evolution admitted discovery inspection has not completed")?;
        Ok((
            scan.complete
                && scan.pending_streams.is_empty()
                && dead_letters == 0
                && direct_complete,
            scan.pending_streams.len().saturating_add(dead_letters),
        ))
    }

    pub(crate) fn resume_sources(&self) -> Result<(), String> {
        let Some(boundary) = self
            .event_store
            .projection_checkpoint(POLICY_BOUNDARY_ID)
            .map_err(|error| error.to_string())?
        else {
            return Ok(());
        };
        if !boundary
            .payload
            .get("paused")
            .and_then(serde_json::Value::as_bool)
            .ok_or("invalid evolution pause boundary")?
        {
            return Ok(());
        }
        // A failed physical pass is not evidence that every admitted effect
        // settled. Preserve that fact rather than skipping an unknown tail.
        if boundary
            .payload
            .get("physical_error")
            .is_some_and(|error| !error.is_null())
        {
            return Err("evolution_pause_has_unsettled_physical_pass".into());
        }
        if !self.paused_claim_status()?.0 {
            return Err("evolution_pause_has_pending_analysis_claims_or_continuation".into());
        }
        self.repair_dead_letters(REPAIR_BATCH)?;
        if self.failure_index()?.unresolved_count != 0 {
            return Err("evolution_pause_has_pending_dead_letters".into());
        }
        // Fix the resume cut before moving any of the three original cursors.
        // Retrying a partially applied resume reuses this cut, never extending
        // the skipped interval to include events that arrived during recovery.
        let cut = match boundary.payload.get("resume_cursor") {
            Some(value) => value.as_u64().ok_or("invalid evolution resume cursor")?,
            None => {
                let cut = self
                    .event_store
                    .durable_commit_cursor()
                    .map_err(|error| error.to_string())?;
                let mut payload = boundary.payload.clone();
                payload["resume_cursor"] = serde_json::json!(cut);
                self.event_store
                    .compare_and_put_projection_checkpoint(
                        POLICY_BOUNDARY_ID,
                        boundary.source_cursor,
                        boundary.revision,
                        &payload,
                        now_ms(),
                    )
                    .map_err(|error| error.to_string())?;
                cut
            }
        };
        for id in [
            PROJECTOR_ID,
            LEGACY_BOOTSTRAP_ID,
            crate::evolution::collaboration_experience::PROJECTOR_ID,
        ] {
            let previous = self
                .event_store
                .projection_checkpoint(id)
                .map_err(|error| error.to_string())?;
            if previous
                .as_ref()
                .is_some_and(|entry| entry.source_cursor > cut)
            {
                return Err(format!(
                    "evolution resume source cursor advanced concurrently: {id}"
                ));
            }
            if previous.as_ref().is_some_and(|entry| {
                entry.source_cursor == cut
                    && entry.payload["resumed_at_cursor"].as_u64() == Some(cut)
            }) {
                continue;
            }
            self.event_store.put_projection_checkpoint_retrying(id, cut,
                &serde_json::json!({"source_cursor": cut, "transaction_index": u32::MAX,
                    "skipped_after_cursor": previous.as_ref().map_or(0, |entry| entry.source_cursor),
                    "paused_at_cursor": boundary.source_cursor, "resumed_at_cursor": cut}), now_ms())
                .map_err(|error| error.to_string())?;
        }
        self.event_store
            .put_projection_checkpoint_retrying(
                POLICY_BOUNDARY_ID,
                cut,
                &serde_json::json!({"paused": false, "paused_at_cursor": boundary.source_cursor,
                "resumed_at_cursor": cut}),
                now_ms(),
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn health(&self) -> Result<EvolutionProjectorHealth, String> {
        self.health_with_worker(false, 0)
    }

    pub(crate) fn health_with_worker(
        &self,
        worker_running: bool,
        consecutive_failures: u32,
    ) -> Result<EvolutionProjectorHealth, String> {
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
        Ok(EvolutionProjectorHealth {
            source_cursor,
            latest_commit_cursor,
            lag_commits,
            dead_letter_count,
            worker_running,
            consecutive_failures,
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
        self.ensure_source_epoch()?;
        let max_commits = max_commits.clamp(1, MAX_SCAN_EVENTS);
        let control = self
            .event_store
            .projection_checkpoint(POLICY_BOUNDARY_ID)
            .map_err(|error| error.to_string())?;
        let mut fresh_page = None;
        let pending = control
            .as_ref()
            .and_then(|entry| entry.payload.get("pending_page"))
            .filter(|value| !value.is_null());
        let admitted: EvolutionAdmittedSourcePage = if let Some(pending) = pending {
            serde_json::from_value(pending.clone())
                .map_err(|error| format!("invalid evolution admitted source page: {error}"))?
        } else {
            if control
                .as_ref()
                .is_some_and(|entry| entry.payload["paused"] == true)
            {
                return Ok(0);
            }
            let cursor = self.cursor()?;
            let page = self
                .event_store
                .projection_scan_page(
                    cursor,
                    &projection_interest(),
                    max_commits.max(1),
                    MAX_SCAN_EVENTS,
                    MAX_SCAN_BYTES,
                )
                .map_err(|error| error.to_string())?;
            let legacy_checkpoint = self
                .event_store
                .projection_checkpoint(LEGACY_BOOTSTRAP_ID)
                .map_err(|error| error.to_string())?;
            let legacy_after = legacy_checkpoint.as_ref().and_then(|entry| {
                entry.payload["transaction_index"]
                    .as_u64()
                    .and_then(|index| u32::try_from(index).ok())
                    .map(|index| (entry.source_cursor, index))
            });
            let legacy = self
                .event_store
                .list_scope_kind_page_asc(
                    RuntimeEventScope::Evolution,
                    "evolution.signal.recorded.v1",
                    legacy_after,
                    PROJECTOR_BATCH,
                )
                .map_err(|error| error.to_string())?;
            if page.scanned_through_cursor == cursor && legacy.is_empty() {
                self.discovery.resume_auto_cases(REPAIR_BATCH)?;
                self.repair_dead_letters(REPAIR_BATCH)?;
                return Ok(0);
            }
            let admitted = EvolutionAdmittedSourcePage {
                source_upper_cursor: page.scanned_through_cursor,
                legacy_upper_position: legacy
                    .last()
                    .map(|event| (event.commit_cursor, event.transaction_index)),
                max_commits: max_commits.max(1),
            };
            fresh_page = Some(page);
            let mut payload = control.as_ref().map_or_else(
                || serde_json::json!({"paused":false}),
                |entry| entry.payload.clone(),
            );
            payload["pending_page"] =
                serde_json::to_value(&admitted).map_err(|error| error.to_string())?;
            self.event_store
                .compare_and_put_projection_checkpoint(
                    POLICY_BOUNDARY_ID,
                    control.as_ref().map_or(0, |entry| entry.source_cursor),
                    control.as_ref().map_or(0, |entry| entry.revision),
                    &payload,
                    now_ms(),
                )
                .map_err(|error| error.to_string())?;
            admitted
        };
        if admitted.max_commits == 0 || admitted.max_commits > MAX_SCAN_EVENTS {
            return Err("evolution admitted source page has an invalid bound".into());
        }
        let cursor = self.cursor()?;
        let head = self
            .event_store
            .durable_commit_cursor()
            .map_err(|error| error.to_string())?;
        if cursor > admitted.source_upper_cursor
            || admitted.source_upper_cursor > head
            || admitted
                .legacy_upper_position
                .is_some_and(|(cursor, _)| cursor > head)
        {
            return Err("evolution admitted source page has an invalid source cut".into());
        }
        self.bootstrap_legacy_signals_through(PROJECTOR_BATCH, admitted.legacy_upper_position)?;
        self.discovery.resume_auto_cases(REPAIR_BATCH)?;
        self.repair_dead_letters(REPAIR_BATCH)?;
        let page = match fresh_page {
            Some(page) => page,
            None => self
                .event_store
                .projection_scan_page(
                    cursor,
                    &projection_interest(),
                    admitted.max_commits,
                    MAX_SCAN_EVENTS,
                    MAX_SCAN_BYTES,
                )
                .map_err(|error| error.to_string())?,
        };
        let mut processed = 0;
        for batch in page
            .batches
            .iter()
            .filter(|batch| batch.commit_cursor <= admitted.source_upper_cursor)
        {
            for event in &batch.events {
                self.process_source(event, batch.commit_cursor)?;
            }
            processed += 1;
        }
        let through = page
            .scanned_through_cursor
            .min(admitted.source_upper_cursor);
        if through > cursor {
            self.checkpoint(through)?;
        }
        if through != admitted.source_upper_cursor {
            return Err("evolution admitted source page did not reach its original cut".into());
        }
        let checkpoint = self
            .event_store
            .projection_checkpoint(POLICY_BOUNDARY_ID)
            .map_err(|error| error.to_string())?
            .ok_or("evolution admitted source descriptor disappeared")?;
        if checkpoint.payload.get("pending_page")
            != Some(&serde_json::to_value(&admitted).map_err(|error| error.to_string())?)
        {
            return Err("evolution admitted source descriptor changed concurrently".into());
        }
        let mut payload = checkpoint.payload.clone();
        payload
            .as_object_mut()
            .ok_or("invalid evolution source descriptor")?
            .remove("pending_page");
        self.event_store
            .compare_and_put_projection_checkpoint(
                POLICY_BOUNDARY_ID,
                checkpoint.source_cursor,
                checkpoint.revision,
                &payload,
                now_ms(),
            )
            .map_err(|error| error.to_string())?;
        Ok(processed)
    }

    fn bootstrap_legacy_signals_through(
        &self,
        limit: usize,
        through: Option<(u64, u32)>,
    ) -> Result<usize, String> {
        let Some(through) = through else {
            return Ok(0);
        };
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
        let events = events
            .into_iter()
            .filter(|event| (event.commit_cursor, event.transaction_index) <= through)
            .collect::<Vec<_>>();
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
                });
            let signal = match signal {
                Ok(signal) => signal,
                Err(error) => {
                    tracing::warn!(
                        source_event_id = %event.event_id,
                        %error,
                        "isolated malformed legacy evolution signal while advancing bootstrap"
                    );
                    continue;
                }
            };
            let signal_id = signal.signal_id.clone();
            let imported = match self.discovery.materialize_projected_signal(signal) {
                Ok(imported) => imported,
                Err(error) => {
                    tracing::warn!(
                        source_event_id = %event.event_id,
                        %signal_id,
                        %error,
                        "isolated invalid legacy evolution signal while advancing bootstrap"
                    );
                    continue;
                }
            };
            if imported.signal_id == signal_id {
                tracing::debug!(
                    source_event_id = %event.event_id,
                    %signal_id,
                    "evolution legacy signal is represented by the canonical signal identity"
                );
            }
        }
        self.event_store
            .put_projection_checkpoint_retrying(
                LEGACY_BOOTSTRAP_ID,
                last.commit_cursor,
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
        let error = last_error.unwrap_or_else(|| "unknown projection failure".to_string());
        self.dead_letter(event, source_cursor, &error)?;
        if event.kind == "goal.observation" {
            return Err(error);
        }
        Ok(())
    }

    fn project_source(&self, event: &DurableRuntimeEvent) -> Result<bool, String> {
        let resolved;
        let event = if event.kind == "goal.observation" {
            if event.scope != RuntimeEventScope::Goal {
                return Err("goal observation source has invalid scope".into());
            }
            let observation = self.event_store.resolve_observation_record(event)?;
            resolved = DurableRuntimeEvent {
                payload: serde_json::json!({"observation":observation}),
                ..event.clone()
            };
            &resolved
        } else {
            event
        };
        let signal = if event.kind == OUTCOME_EVENT_KIND {
            outcome_signal(event)?
        } else if event.kind == "agent.run_evaluated" {
            self.repeated_agent_failure_signal(event)?
        } else if matches!(
            event.kind.as_str(),
            "tool.invocation.completed" | "tool.invocation.failed"
        ) {
            self.tool_experience_signal(event)?
        } else {
            signal_from_source_event(event)
        };
        let Some(signal) = signal else {
            return Ok(false);
        };
        self.discovery
            .materialize_projected_signal(signal)
            .map(|_| true)
    }

    fn tool_experience_signal(
        &self,
        event: &DurableRuntimeEvent,
    ) -> Result<Option<EvolutionSignal>, String> {
        let workspace = self
            .workspace_identity
            .as_ref()
            .ok_or("tool experience requires a trusted workspace identity")?;
        let invocation: crate::ToolInvocationRecord = serde_json::from_value(event.payload.clone())
            .map_err(|error| format!("tool experience invocation invalid: {error}"))?;
        let binding = event
            .activity_binding()
            .ok_or("tool experience requires immutable Runtime activity ownership")?;
        if event.scope != RuntimeEventScope::Tool
            || event.stream_id != format!("session:{}", invocation.session_id)
            || binding.session_id != invocation.session_id
            || binding.tool_call_id.as_deref() != Some(invocation.tool_call_id.as_str())
            || binding.tool_contract_id.as_deref() != Some(invocation.tool_name.as_str())
            || invocation.ended_at_ms.is_none()
            || (event.kind == "tool.invocation.completed"
                && invocation.status != crate::ToolInvocationStatus::Completed)
            || (event.kind == "tool.invocation.failed"
                && !matches!(
                    invocation.status,
                    crate::ToolInvocationStatus::Failed | crate::ToolInvocationStatus::TimedOut
                ))
        {
            return Err("tool experience terminal event does not match its Runtime binding".into());
        }
        let Some(candidate) = crate::memory_candidate_from_tool_invocation(
            &invocation,
            &crate::ToolMemoryCandidatePolicy::default(),
        ) else {
            return Ok(None);
        };
        let failed = invocation.status != crate::ToolInvocationStatus::Completed;
        let digest = format!("{:x}", Sha256::digest(event.event_id.as_bytes()));
        Ok(Some(EvolutionSignal {
            signal_id:format!("evo-tool-source-{}",&digest[..24]),
            signal_type:if failed {EvolutionSignalType::RecoveryGap} else {EvolutionSignalType::SlowProgress},
            severity:EvolutionSignalSeverity::Warning,
            source:EvolutionSignalSource {owner:scope_owner(event.scope).into(),session_id:Some(binding.session_id),
                agent_id:binding.agent_instance_id,team_id:binding.team_run_id,run_id:Some(binding.root_execution_id)},
            evidence_refs:vec![EvidenceRef::observed("runtime_event",event.event_id.clone()).with_source(event.kind.clone())],
            // Do not copy candidate.reason: it contains private tool previews.
            summary:format!("{}: {} ({})",candidate.summary,invocation.tool_name,invocation.status.as_str()),
            suggested_action:"Inspect the scoped original tool evidence; propose a retrieval/tool-use candidate and compare it with the pinned baseline before any release. An observation is not a verified method.".into(),
            immediate_task_can_continue:true,
            scope:EvolutionSignalScope {workspace_identity:workspace.clone(),affected_subject:format!("runtime.tool:{}",invocation.tool_name),
                workload_fingerprint:format!("tool-contract:{}",invocation.effective_registration_id),
                config_definition_revision:format!("tool-contract-v{}",invocation.contract_version),
                provider:"not_applicable".into(),model:"not_applicable".into(),evaluation_environment:"production".into()},
            created_at_ms:u128::from(event.created_at_ms),
        }))
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
            let expected_revision = self
                .event_store
                .stream_revision(PROJECTOR_STREAM)
                .map_err(|error| error.to_string())?;
            if self
                .event_store
                .event_by_idempotency_key(PROJECTOR_STREAM, &key)
                .map_err(|error| error.to_string())?
                .is_some()
            {
                return Ok(());
            }
            let mut index = self.failure_index()?;
            let head = self
                .failure_catalog_page(&index, index.head_page)?
                .source_event_ids
                .get(index.head_offset)
                .cloned();
            if head.as_deref() != Some(source.event_id.as_str()) {
                if self
                    .event_store
                    .event_by_idempotency_key(PROJECTOR_STREAM, &key)
                    .map_err(|error| error.to_string())?
                    .is_some()
                {
                    return Ok(());
                }
                if self
                    .event_store
                    .stream_revision(PROJECTOR_STREAM)
                    .map_err(|error| error.to_string())?
                    != expected_revision
                {
                    continue;
                }
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
                Err(error) => {
                    if self
                        .event_store
                        .event_by_idempotency_key(PROJECTOR_STREAM, &key)
                        .map_err(|lookup_error| lookup_error.to_string())?
                        .is_some()
                    {
                        return Ok(());
                    }
                    if matches!(error, crate::RuntimeEventStoreError::StaleRevision { .. }) {
                        continue;
                    }
                    return Err(error.to_string());
                }
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
            let expected_revision = self
                .event_store
                .stream_revision(PROJECTOR_STREAM)
                .map_err(|store_error| store_error.to_string())?;
            if self
                .event_store
                .event_by_idempotency_key(PROJECTOR_STREAM, &key)
                .map_err(|store_error| store_error.to_string())?
                .is_some()
            {
                return Ok(());
            }
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
                Err(error) => {
                    if self
                        .event_store
                        .event_by_idempotency_key(PROJECTOR_STREAM, &key)
                        .map_err(|lookup_error| lookup_error.to_string())?
                        .is_some()
                    {
                        return Ok(());
                    }
                    if matches!(error, crate::RuntimeEventStoreError::StaleRevision { .. }) {
                        continue;
                    }
                    return Err(error.to_string());
                }
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
        session_id: if event.kind == "goal.observation" {
            event
                .payload
                .get("observation")
                .and_then(|value| {
                    serde_json::from_value::<harness_contract::goal::RuntimeObservation>(
                        value.clone(),
                    )
                    .ok()
                })
                .map(|observation| observation.identity.session_id)
        } else {
            ref_id(event, "session")
        },
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
            let observation: harness_contract::goal::RuntimeObservation =
                serde_json::from_value(event.payload.get("observation")?.clone()).ok()?;
            let failed = observation.failed();
            let tool_calls = observation.cost_delta.tool_calls;
            let verified_gain = observation.has_verified_gain();
            if failed {
                Some((
                    EvolutionSignalType::AgentFailurePattern,
                    EvolutionSignalSeverity::Warning,
                    "A typed Goal observation recorded an execution failure.".to_string(),
                    "Cluster attributable failures and run a frozen recovery scenario.".to_string(),
                    true,
                ))
            } else if tool_calls > 0 && !verified_gain {
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

fn projection_interest() -> RuntimeProjectionInterest {
    let mut interests = [
        RuntimeEventScope::Agent,
        RuntimeEventScope::Team,
        RuntimeEventScope::Tool,
        RuntimeEventScope::Task,
    ]
    .map(|scope| RuntimeProjectionEventInterest::new(scope, OUTCOME_EVENT_KIND))
    .to_vec();
    interests.extend([
        RuntimeProjectionEventInterest::new(RuntimeEventScope::Tool, "tool.invocation.completed"),
        RuntimeProjectionEventInterest::new(RuntimeEventScope::Tool, "tool.invocation.failed"),
        RuntimeProjectionEventInterest::new(RuntimeEventScope::Evolution, "agent.run_evaluated"),
        RuntimeProjectionEventInterest::new(RuntimeEventScope::Goal, "goal.intervention"),
        RuntimeProjectionEventInterest::new(RuntimeEventScope::Goal, "goal.observation"),
        RuntimeProjectionEventInterest::new(RuntimeEventScope::Goal, "goal.completed"),
    ]);
    RuntimeProjectionInterest::new(interests)
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

    use crate::runtime_event_store::*;
    use std::sync::Mutex;

    #[test]
    fn policy_resume_refuses_unknown_physical_exit_and_malformed_pause_boundary() {
        for malformed in [false, true] {
            let store = Arc::new(RuntimeEventStore::for_test());
            let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&store)));
            let projector = EvolutionSignalProjector::new(Arc::clone(&store), discovery);
            if malformed {
                store
                    .put_projection_checkpoint(
                        POLICY_BOUNDARY_ID,
                        0,
                        &serde_json::json!({"paused":"true"}),
                        1,
                    )
                    .unwrap();
            } else {
                projector
                    .pause_sources(Some("original pass failed without proven settlement"))
                    .unwrap();
            }
            assert!(projector.resume_sources().is_err());
            assert!(store.projection_checkpoint(PROJECTOR_ID).unwrap().is_none());
            assert!(store
                .projection_checkpoint(LEGACY_BOOTSTRAP_ID)
                .unwrap()
                .is_none());
            assert!(store
                .projection_checkpoint(crate::evolution::collaboration_experience::PROJECTOR_ID)
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn interrupted_policy_resume_keeps_drain_evidence_and_the_original_resume_cut() {
        let backend = Arc::new(ObservationWorkerBackend {
            inner: EphemeralRuntimeEventStore::new(),
            faults: Mutex::new(ObservationWorkerFaults::default()),
        });
        let events = Arc::new(RuntimeEventStore::from_backend(backend.clone()));
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = EvolutionSignalProjector::new(Arc::clone(&events), discovery);
        projector.pause_sources(None).unwrap();
        projector.reconcile_paused_claims().unwrap();
        assert_eq!(projector.paused_claim_status().unwrap(), (true, 0));
        backend.faults.lock().unwrap().fail_checkpoint = true;
        assert!(projector
            .resume_sources()
            .unwrap_err()
            .contains("checkpoint write interrupted"));
        let interrupted = events
            .projection_checkpoint(POLICY_BOUNDARY_ID)
            .unwrap()
            .unwrap();
        let cut = interrupted.payload["resume_cursor"].as_u64().unwrap();
        assert_eq!(interrupted.payload["direct_signal_scan_complete"], true);
        assert_eq!(projector.paused_claim_status().unwrap(), (true, 0));
        events
            .append(RuntimeEventInput {
                stream_id: "foreground:during-policy-resume".into(),
                scope: RuntimeEventScope::Task,
                kind: "fixture.foreground.accepted".into(),
                status: None,
                actor: Some("fixture".into()),
                refs: Vec::new(),
                payload: serde_json::json!({"value":"after-frozen-resume-cut"}),
            })
            .unwrap();
        assert!(events.durable_commit_cursor().unwrap() > cut);
        // The original reader retries its interrupted cursor write; the test
        // does not re-run a projector page or synthesize a settlement receipt.
        projector.resume_sources().unwrap();
        for id in [
            PROJECTOR_ID,
            LEGACY_BOOTSTRAP_ID,
            crate::evolution::collaboration_experience::PROJECTOR_ID,
        ] {
            assert_eq!(
                events
                    .projection_checkpoint(id)
                    .unwrap()
                    .unwrap()
                    .source_cursor,
                cut
            );
        }
        let resumed = events
            .projection_checkpoint(POLICY_BOUNDARY_ID)
            .unwrap()
            .unwrap();
        assert_eq!(resumed.payload["paused"], false);
        assert_eq!(resumed.payload["resumed_at_cursor"], cut);
    }

    #[derive(Debug, Default)]
    struct ObservationWorkerFaults {
        replacement: Option<DurableRuntimeEvent>,
        lose_dead_letter: bool,
        lose_recovery: bool,
        stale_dead_letter: bool,
        stale_recovery: bool,
        fail_checkpoint: bool,
    }

    #[derive(Debug)]
    struct ObservationWorkerBackend {
        inner: EphemeralRuntimeEventStore,
        faults: Mutex<ObservationWorkerFaults>,
    }

    impl ObservationWorkerBackend {
        fn check_checkpoint_write(&self, projection_id: &str) -> RuntimeEventStoreResult<()> {
            if projection_id == PROJECTOR_ID
                && std::mem::take(&mut self.faults.lock().unwrap().fail_checkpoint)
            {
                return Err(crate::RuntimeEventStoreError::Corrupt(
                    "checkpoint write interrupted".into(),
                ));
            }
            Ok(())
        }

        fn replace(&self, event: &mut DurableRuntimeEvent) {
            if let Some(replacement) = self.faults.lock().unwrap().replacement.as_ref() {
                if replacement.event_id == event.event_id {
                    *event = replacement.clone();
                }
            }
        }
    }

    macro_rules! forward_observation_worker_ports {
        ($(fn $name:ident(&self $(, $argument:ident: $kind:ty)*) -> $result:ty;)*) => {
            $(fn $name(&self $(, $argument: $kind)*) -> $result {
                self.inner.$name($($argument),*)
            })*
        };
    }

    impl RuntimeEventStoreBackend for ObservationWorkerBackend {
        fn append_transaction(
            &self,
            request: AppendTransactionRequest,
        ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
            let dead_letter = request
                .events
                .iter()
                .any(|input| input.event.kind == FAILED_KIND);
            let receipt = self.inner.append_transaction(request)?;
            if dead_letter && std::mem::take(&mut self.faults.lock().unwrap().stale_dead_letter) {
                return Err(crate::RuntimeEventStoreError::StaleRevision {
                    stream_id: PROJECTOR_STREAM.into(),
                    expected: 0,
                    actual: 1,
                });
            }
            if dead_letter && std::mem::take(&mut self.faults.lock().unwrap().lose_dead_letter) {
                return Err(crate::RuntimeEventStoreError::Corrupt(
                    "lost dead-letter acknowledgement".into(),
                ));
            }
            Ok(receipt)
        }

        fn append_batch_if_revision(
            &self,
            stream_id: String,
            expected_revision: u64,
            transaction_id: String,
            events: Vec<RuntimeTransactionEventInput>,
        ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
            let recovery = events
                .iter()
                .any(|input| input.event.kind == RECOVERED_KIND);
            let receipt = self.inner.append_batch_if_revision(
                stream_id,
                expected_revision,
                transaction_id,
                events,
            )?;
            if recovery && std::mem::take(&mut self.faults.lock().unwrap().stale_recovery) {
                return Err(crate::RuntimeEventStoreError::StaleRevision {
                    stream_id: PROJECTOR_STREAM.into(),
                    expected: 0,
                    actual: 1,
                });
            }
            if recovery && std::mem::take(&mut self.faults.lock().unwrap().lose_recovery) {
                return Err(crate::RuntimeEventStoreError::Corrupt(
                    "lost recovery acknowledgement".into(),
                ));
            }
            Ok(receipt)
        }

        fn events_after_cursor(
            &self,
            cursor: u64,
            max_commits: usize,
        ) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>> {
            let mut batches = self.inner.events_after_cursor(cursor, max_commits)?;
            for batch in &mut batches {
                for event in &mut batch.events {
                    self.replace(event);
                }
            }
            Ok(batches)
        }

        fn projection_scan_page(
            &self,
            cursor: u64,
            interest: &RuntimeProjectionInterest,
            max_commits: usize,
            max_events: usize,
            max_bytes: usize,
        ) -> RuntimeEventStoreResult<RuntimeProjectionScanPage> {
            let mut page = self.inner.projection_scan_page(
                cursor,
                interest,
                max_commits,
                max_events,
                max_bytes,
            )?;
            for batch in &mut page.batches {
                for event in &mut batch.events {
                    self.replace(event);
                }
            }
            Ok(page)
        }

        fn put_projection_checkpoint(
            &self,
            projection_id: &str,
            source_cursor: u64,
            payload: &serde_json::Value,
            updated_at_ms: u64,
        ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
            self.check_checkpoint_write(projection_id)?;
            self.inner.put_projection_checkpoint(
                projection_id,
                source_cursor,
                payload,
                updated_at_ms,
            )
        }

        fn compare_and_put_projection_checkpoint(
            &self,
            projection_id: &str,
            source_cursor: u64,
            expected_revision: u64,
            payload: &serde_json::Value,
            updated_at_ms: u64,
        ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
            self.check_checkpoint_write(projection_id)?;
            self.inner.compare_and_put_projection_checkpoint(
                projection_id,
                source_cursor,
                expected_revision,
                payload,
                updated_at_ms,
            )
        }

        forward_observation_worker_ports! {
            fn recent_event_lines(&self, stream_id: &str, references: &[RuntimeEventRef], through_cursor: u64, max_events: usize, max_bytes: usize) -> Result<Vec<harness_contract::mission::MissionControlEventLine>, String>;
            fn background_projection_capacity_hint(&self) -> usize;
            fn prepared_content_root(&self, origin: &crate::PreparedArtifactOrigin, artifact: &harness_contract::context::ArtifactRef) -> RuntimeEventStoreResult<PreparedContentRoot>;
            fn transaction_stream_revision_exact(&self, transaction_id: &str, stream_id: &str) -> RuntimeEventStoreResult<Option<CommittedStreamRevision>>;
            fn append(&self, input: RuntimeEventInput) -> Result<crate::DurableRuntimeEvent, String>;
            fn append_transaction_with_terminal(&self, request: AppendTransactionRequest, terminal: SessionTerminalInput) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
            fn consume_verified_decision_lease(&self, lease_id: &str, principal_id: &str, review_id: &str, action: &str, scope: &str, evidence_digest: &str, credential_epoch: u64, consumed_at_ms: u64) -> RuntimeEventStoreResult<()>;
            fn append_transaction_with_verified_decision_lease(&self, request: AppendTransactionRequest, lease: &crate::VerifiedDecisionLease) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
            fn projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>>;
            fn projection_checkpoints_with_prefix(&self, prefix: &str) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>>;
            fn compare_and_repair_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, expected_revision: u64, payload: &serde_json::Value, updated_at_ms: u64) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
            fn delete_projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<bool>;
            fn event_by_idempotency_key(&self, stream_id: &str, idempotency_key: &str) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>>;
            fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64>;
            fn list_stream(&self, stream_id: &str) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn count_stream_kinds(&self, stream_id: &str, kinds: &[&str], through_sequence: u64) -> Result<u64, String>;
            fn list_stream_page_desc(&self, stream_id: &str, limit: usize, offset: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn stream_event_count(&self, stream_id: &str) -> Result<usize, String>;
            fn execution_events_for_session(&self, session_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn events_for_root_execution(&self, root_execution_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn events_for_root_execution_kind(&self, root_execution_id: &str, kind: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn events_for_activity(&self, activity_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn list_scope(&self, scope: RuntimeEventScope, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn list_scope_page_asc(&self, scope: RuntimeEventScope, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn list_scope_stream_prefix_page_asc(&self, scope: RuntimeEventScope, stream_prefix: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn list_scope_kind_page_asc(&self, scope: RuntimeEventScope, kind: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn stream_ids_for_scope(&self, scope: RuntimeEventScope) -> RuntimeEventStoreResult<Vec<String>>;
            fn stream_ids_for_scope_kind_at_sequence(&self, scope: RuntimeEventScope, kind: &str, sequence: u64) -> RuntimeEventStoreResult<Vec<String>>;
            fn stream_ids_for_scope_kind_at_sequence_page(&self, scope: RuntimeEventScope, kind: &str, sequence: u64, after: Option<(u64, String)>, limit: usize) -> RuntimeEventStoreResult<Vec<(String, u64)>>;
            fn latest_stream_statuses_for_scope_kind_at_sequence(&self, scope: RuntimeEventScope, kind: &str, sequence: u64) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>>;
            fn all_events(&self, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
            fn durable_commit_cursor(&self) -> RuntimeEventStoreResult<u64>;
            fn latest_for_stream(&self, stream_id: &str) -> Result<Option<crate::DurableRuntimeEvent>, String>;
            fn latest_for_stream_kind(&self, stream_id: &str, kind: &str) -> Result<Option<crate::DurableRuntimeEvent>, String>;
            fn enqueue_session_terminal(&self, terminal_id: &str, message_id: &str, session_id: &str, commit_cursor: u64, payload_ref: &str) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn claim_session_terminals(&self, worker_id: &str, now_ms: u64, lease_ms: u64, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
            fn session_terminal(&self, terminal_id: &str) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>>;
            fn has_unsettled_session_terminals(&self, session_id: &str) -> RuntimeEventStoreResult<bool>;
            fn session_terminal_for_execution(&self, session_id: &str, execution_id: &str, turn_id: &str) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>>;
            fn materialized_session_terminals_after(&self, session_id: &str, after_commit_cursor: u64, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
            fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth>;
            fn blocked_session_terminals(&self, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
            fn retry_session_terminal(&self, terminal_id: &str, actor: &str, reason: &str, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn adopt_session_terminal_fence(&self, request: &RuntimeSessionTerminalFenceAdoption) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn ack_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn suppress_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, reason: &str, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn fail_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, class: RuntimeSessionOutboxFailureClass, error: &str, retry_at_ms: u64, max_attempts: u32, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn position_page(&self, scope: RuntimeEventScope, identity: &str, kind: Option<&str>, after: Option<(u64, u32)>, through_cursor: u64, max_events: usize, max_bytes: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
            fn list_stream_after(&self, stream_id: &str, after_sequence: u64, through_sequence: u64, max_events: usize, max_bytes: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
            fn list_stream_before(&self, stream_id: &str, through_sequence: u64, max_events: usize, max_bytes: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
        }
    }

    struct ObservationWorkerFixture {
        events: Arc<RuntimeEventStore>,
        discovery: Arc<EvolutionDiscoveryService>,
        backend: Arc<ObservationWorkerBackend>,
        source: DurableRuntimeEvent,
        blob: Option<(std::path::PathBuf, Vec<u8>)>,
    }

    impl ObservationWorkerFixture {
        fn new(
            large: bool,
            result: &str,
            tool_calls: u64,
            failure: Option<&str>,
            gain: bool,
        ) -> Self {
            let root = std::path::PathBuf::from(
                std::env::var_os("COWD_R1_06_FIXTURE_ROOT")
                    .expect("explicit private plan fixture root"),
            )
            .join(format!("observation-worker-{}", uuid::Uuid::new_v4()));
            assert!(
                root.is_absolute()
                    && root.starts_with(
                        "/media/yi/Datas/workspace/plan/2026-10/1016-cowd-future-kernel"
                    )
            );
            let backend = Arc::new(ObservationWorkerBackend {
                inner: EphemeralRuntimeEventStore::new(),
                faults: Mutex::new(ObservationWorkerFaults::default()),
            });
            let events = Arc::new(RuntimeEventStore::from_backend(backend.clone()));
            let artifact_root = root.join("artifacts");
            events
                .bind_artifact_store(Arc::new(
                    crate::ArtifactStore::for_test(
                        artifact_root.clone(),
                        crate::ArtifactStoreConfig {
                            compact_threshold_bytes: 1024,
                            ..Default::default()
                        },
                    )
                    .unwrap(),
                ))
                .unwrap();
            let goals = crate::execution_core::GoalStore::new(events.clone());
            let goal: harness_contract::goal::GoalContract = serde_json::from_value(serde_json::json!({
                "id":"worker-goal","session_id":"worker-session","objective":"worker oracle",
                "criteria":[{"id":"checked","statement":"result checked","status":"open"}],"constraints":[],"phase":"execution","evidence_refs":[],"unresolved":[],"blockers":[],
                "scope":"internal","spec_revision":1,"spec_digest":"worker-spec","completion":"open","revision":1,"user_sequence":1
            })).unwrap();
            goals.create(goal).unwrap();
            let observation: harness_contract::goal::RuntimeObservation = serde_json::from_value(serde_json::json!({
                "identity":{"workspace_id":"workspace","session_id":"worker-session","graph_id":"worker-graph","goal_id":"worker-goal","node_id":"worker-node"},
                "kind":"tool_progress","source":"runtime.tool_batch","source_revision":1,
                "freshness":{"observed_at_ms":1,"policy_revision":"goal-observation-v2"},
                "summary":if large {"private worker body ".repeat(2048)} else {"private worker body".into()},
                "fingerprint":"worker-fingerprint","result_class":result,"failure_class":failure,
                "cost_delta":harness_contract::goal::CostDelta {tool_calls,..Default::default()},
                "information_gain":{"distinguishing_evidence_refs":if gain {vec!["observed:worker"]} else {vec![]},"resolved_unknown_refs":[],"provenance":"observed"}
            })).unwrap();
            goals.record_observation(observation).unwrap();
            let source = events
                .latest_for_stream("goal:worker-goal")
                .unwrap()
                .unwrap();
            assert_eq!(source.kind, "goal.observation");
            assert_eq!(
                source.payload["observation_body"]["body"]["kind"],
                if large { "artifact_ref" } else { "inline" }
            );
            let blob = if large {
                let artifact: crate::ArtifactRef = serde_json::from_value(
                    source.payload["observation_body"]["body"]["artifact"].clone(),
                )
                .unwrap();
                let digest = artifact
                    .sha256
                    .strip_prefix("sha256:")
                    .unwrap_or(&artifact.sha256);
                let directory = artifact_root
                    .join("objects")
                    .join(&digest[..2])
                    .join(digest);
                let paths = std::fs::read_dir(directory)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .collect::<Vec<_>>();
                assert_eq!(paths.len(), 1);
                Some((paths[0].clone(), std::fs::read(&paths[0]).unwrap()))
            } else {
                None
            };
            let discovery = Arc::new(EvolutionDiscoveryService::new(events.clone()));
            Self {
                events,
                discovery,
                backend,
                source,
                blob,
            }
        }

        fn projector(&self) -> EvolutionSignalProjector {
            EvolutionSignalProjector::new(self.events.clone(), self.discovery.clone())
        }

        fn assert_counts(&self, failed: usize, recovered: usize, unresolved: u64, signals: usize) {
            assert_eq!(
                self.events
                    .replay_scope_kind(RuntimeEventScope::Evolution, FAILED_KIND)
                    .unwrap()
                    .len(),
                failed
            );
            assert_eq!(
                self.events
                    .replay_scope_kind(RuntimeEventScope::Evolution, RECOVERED_KIND)
                    .unwrap()
                    .len(),
                recovered
            );
            assert_eq!(
                self.projector().failure_index().unwrap().unresolved_count,
                unresolved
            );
            assert_eq!(self.discovery.list_signals().unwrap().len(), signals);
            assert_eq!(self.discovery.list_cases(100).unwrap().len(), signals);
            assert!(self.discovery.list_proposals().unwrap().is_empty());
        }
    }

    #[test]
    fn r1_event_replay_observation_failure_keeps_worker_checkpoint() {
        for (large, fault) in [false, true].into_iter().flat_map(|large| {
            [
                "schema",
                "legacy_schema",
                "event_scope",
                "observation_schema",
                "content_schema",
                "body",
                "typed_body",
                "missing_ref",
                "ref_scope",
                "ref_hash",
                "ref_bytes",
                "classifier",
                "scope",
                "missing_blob",
                "truncated_blob",
                "flipped_blob",
            ]
            .into_iter()
            .filter(move |fault| large || !(fault.contains("ref") || fault.ends_with("blob")))
            .map(move |fault| (large, fault))
        }) {
            let fixture = ObservationWorkerFixture::new(large, "failed", 1, Some("tool"), false);
            let projector = fixture.projector();
            let checkpoint = fixture.events.projection_checkpoint(PROJECTOR_ID).unwrap();
            let mut corrupt = fixture.source.clone();
            match fault {
                "schema" => corrupt.schema_version = 999,
                "legacy_schema" => corrupt.schema_version = 1,
                "event_scope" => corrupt.scope = RuntimeEventScope::Session,
                "observation_schema" => {
                    corrupt.payload["observation_schema"] = serde_json::json!(999)
                }
                "content_schema" => {
                    corrupt.payload["observation_body"]["content_schema"] = serde_json::json!(999)
                }
                "body" => {
                    corrupt
                        .payload
                        .as_object_mut()
                        .unwrap()
                        .remove("observation_body");
                }
                "typed_body" => {
                    let mut value = fixture
                        .events
                        .resolve_observation_record(&fixture.source)
                        .unwrap();
                    value["kind"] = serde_json::json!("not_a_current_observation_kind");
                    let bytes = serde_json::to_vec(&value).unwrap();
                    corrupt.payload["observation_body"] = serde_json::json!({"content_schema":2,"sha256":format!("sha256:{:x}",Sha256::digest(&bytes)),"bytes":bytes.len(),"body":{"kind":"inline","value":value}});
                }
                "missing_ref" => {
                    corrupt.payload["observation_body"]["body"]
                        .as_object_mut()
                        .unwrap()
                        .remove("artifact");
                }
                "ref_scope" => {
                    corrupt.payload["observation_body"]["body"]["artifact"]["visibility_scope"] =
                        serde_json::json!("session:foreign")
                }
                "ref_hash" => {
                    corrupt.payload["observation_body"]["body"]["artifact"]["sha256"] =
                        serde_json::json!("sha256:bad")
                }
                "ref_bytes" => {
                    corrupt.payload["observation_body"]["body"]["artifact"]["bytes"] =
                        serde_json::json!(1)
                }
                "classifier" => {
                    corrupt.payload["classifier"]["tool_calls"] = serde_json::json!(999)
                }
                "scope" => corrupt.payload["session_id"] = serde_json::json!("foreign-session"),
                "missing_blob" => std::fs::remove_file(&fixture.blob.as_ref().unwrap().0).unwrap(),
                "truncated_blob" => {
                    std::fs::write(&fixture.blob.as_ref().unwrap().0, b"{").unwrap()
                }
                "flipped_blob" => {
                    let (path, bytes) = fixture.blob.as_ref().unwrap();
                    let mut changed = bytes.clone();
                    changed[0] ^= 1;
                    std::fs::write(path, changed).unwrap();
                }
                _ => unreachable!(),
            }
            fixture.backend.faults.lock().unwrap().replacement = Some(corrupt);
            for _ in 0..2 {
                assert!(projector.run_once(128).is_err(), "{fault}");
                assert_eq!(
                    fixture.events.projection_checkpoint(PROJECTOR_ID).unwrap(),
                    checkpoint,
                    "{fault}"
                );
                fixture.assert_counts(1, 0, 1, 0);
            }
            assert!(fixture.projector().run_once(128).is_err(), "{fault}");
            fixture.assert_counts(1, 0, 1, 0);
            let failure = fixture
                .events
                .event_by_idempotency_key(
                    PROJECTOR_STREAM,
                    &format!("dead-letter:{}", fixture.source.event_id),
                )
                .unwrap()
                .unwrap();
            assert_eq!(failure.payload["source_event_id"], fixture.source.event_id);
            assert_eq!(failure.payload["source_kind"], "goal.observation");
            assert_eq!(
                failure.payload["source_cursor"],
                fixture.source.commit_cursor
            );
            assert_eq!(failure.payload["attempts"], MAX_SOURCE_RETRIES);
            if fault.ends_with("blob") {
                let (path, bytes) = fixture.blob.as_ref().unwrap();
                std::fs::write(path, bytes).unwrap();
                fixture.backend.faults.lock().unwrap().replacement = None;
                fixture.projector().run_once(128).unwrap();
                fixture.assert_counts(1, 1, 0, 1);
                fixture.projector().run_once(128).unwrap();
                fixture.assert_counts(1, 1, 0, 1);
            }
        }
    }

    #[test]
    fn r1_event_replay_observation_repair_resumes_exactly_once() {
        for (result, tools, failure, gain, expected_signals) in [
            ("failed", 1, Some("tool"), false, 1),
            ("partial", 1, Some("tool"), false, 1),
            ("succeeded", 1, None, false, 1),
            ("succeeded", 1, None, true, 0),
            ("informational", 0, None, false, 0),
        ] {
            for (large, stale) in [(false, false), (true, false), (false, true), (true, true)] {
                let fixture = ObservationWorkerFixture::new(large, result, tools, failure, gain);
                let projector = fixture.projector();
                let mut corrupt = fixture.source.clone();
                corrupt.payload["classifier"]["tool_calls"] = serde_json::json!(999);
                {
                    let mut faults = fixture.backend.faults.lock().unwrap();
                    faults.replacement = Some(corrupt);
                    faults.lose_dead_letter = !stale;
                    faults.stale_dead_letter = stale;
                }
                assert!(projector.run_once(128).is_err());
                assert!(projector.run_once(128).is_err());
                fixture.assert_counts(1, 0, 1, 0);
                {
                    let mut faults = fixture.backend.faults.lock().unwrap();
                    faults.replacement = None;
                    faults.lose_recovery = !stale;
                    faults.stale_recovery = stale;
                    faults.fail_checkpoint = true;
                }
                assert!(projector.run_once(128).is_err());
                fixture.assert_counts(1, 1, 0, expected_signals);
                assert_eq!(projector.cursor().unwrap(), 0);
                for _ in 0..3 {
                    fixture.projector().run_once(128).unwrap();
                    fixture.assert_counts(1, 1, 0, expected_signals);
                }
                assert!(fixture.projector().cursor().unwrap() >= fixture.source.commit_cursor);
                let recovered = fixture
                    .events
                    .replay_scope_kind(RuntimeEventScope::Evolution, RECOVERED_KIND)
                    .unwrap();
                assert_eq!(
                    recovered[0].payload["source_event_id"],
                    fixture.source.event_id
                );
                assert_eq!(recovered[0].payload["projected"], expected_signals == 1);
                if expected_signals == 1 {
                    let signals = fixture.discovery.list_signals().unwrap();
                    assert_eq!(
                        signals[0].signal_type,
                        if result == "succeeded" {
                            EvolutionSignalType::LowNoveltyToolLoop
                        } else {
                            EvolutionSignalType::AgentFailurePattern
                        }
                    );
                    assert_eq!(signals[0].severity, EvolutionSignalSeverity::Warning);
                    assert_eq!(
                        signals[0].source.session_id.as_deref(),
                        Some("worker-session")
                    );
                    assert_eq!(
                        signals[0].source.owner,
                        fixture.source.actor.clone().unwrap()
                    );
                    assert_eq!(
                        signals[0].evidence_refs,
                        vec![EvidenceRef::observed(
                            "runtime_event",
                            fixture.source.event_id.clone()
                        )
                        .with_source("goal.observation")]
                    );
                    assert_eq!(signals[0].scope.affected_subject, "goal:worker-goal");
                    assert_eq!(
                        signals[0].scope.config_definition_revision,
                        "event-schema-v2"
                    );
                    assert!(signals[0].immediate_task_can_continue);
                    let cases = fixture.discovery.list_cases(100).unwrap();
                    assert_eq!(cases[0].state, crate::EvolutionCaseState::Open);
                    assert_eq!(cases[0].recurrence_count, 1);
                    assert_eq!(cases[0].critical_count, 0);
                    assert_eq!(cases[0].signal_ids, vec![signals[0].signal_id.clone()]);
                    assert_eq!(cases[0].evidence_refs, signals[0].evidence_refs);
                    assert_eq!(cases[0].key.affected_subject, "goal:worker-goal");
                    assert!(!signals[0].summary.contains("private worker body"));
                }
            }
        }
    }

    #[test]
    fn r1_event_replay_observation_failure_stops_same_commit_successor() {
        let fixture = ObservationWorkerFixture::new(false, "failed", 1, Some("tool"), false);
        let goals = crate::execution_core::GoalStore::new(fixture.events.clone());
        let mut observation: harness_contract::goal::RuntimeObservation = serde_json::from_value(
            fixture
                .events
                .resolve_observation_record(&fixture.source)
                .unwrap(),
        )
        .unwrap();
        observation.source_revision = 2;
        observation.fingerprint = "second-worker-source".into();
        let input = goals
            .observation_event(&observation, "same-commit-observation".into())
            .unwrap();
        let revision = fixture.events.stream_revision("goal:worker-goal").unwrap();
        fixture.events.append_batch_if_revision("goal:worker-goal",revision,"same-commit-source",vec![
            input,
            RuntimeTransactionEventInput {
                event:RuntimeEventInput {stream_id:"goal:worker-goal".into(),scope:RuntimeEventScope::Goal,kind:"goal.intervention".into(),
                    status:Some("replan".into()),actor:Some("runtime.intervention_policy".into()),refs:vec![],
                    payload:serde_json::json!({"intervention":{"reason":"repeated low novelty reads"}})},
                idempotency_key:Some("same-commit-successor".into()),schema_version:1,
            },
        ]).unwrap();
        let source = fixture
            .events
            .event_by_idempotency_key("goal:worker-goal", "same-commit-observation")
            .unwrap()
            .unwrap();
        let successor = fixture
            .events
            .event_by_idempotency_key("goal:worker-goal", "same-commit-successor")
            .unwrap()
            .unwrap();
        assert_eq!(source.commit_cursor, successor.commit_cursor);
        assert!(source.transaction_index < successor.transaction_index);
        let mut corrupt = source.clone();
        corrupt.schema_version = 999;
        fixture.backend.faults.lock().unwrap().replacement = Some(corrupt);
        for _ in 0..2 {
            assert!(fixture.projector().run_once(128).is_err());
            assert_eq!(fixture.projector().cursor().unwrap(), 0);
            fixture.assert_counts(1, 0, 1, 1);
            let signals = fixture.discovery.list_signals().unwrap();
            assert_eq!(signals[0].evidence_refs[0].id, fixture.source.event_id);
            assert_eq!(
                fixture
                    .events
                    .event_by_idempotency_key("goal:worker-goal", "same-commit-observation")
                    .unwrap()
                    .unwrap(),
                source
            );
        }
        fixture.backend.faults.lock().unwrap().replacement = None;
        for _ in 0..3 {
            fixture.projector().run_once(128).unwrap();
        }
        assert_eq!(fixture.discovery.list_signals().unwrap().len(), 3);
        assert_eq!(fixture.discovery.list_cases(100).unwrap().len(), 2);
        assert_eq!(
            fixture
                .projector()
                .failure_index()
                .unwrap()
                .unresolved_count,
            0
        );
        assert_eq!(
            fixture
                .events
                .replay_scope_kind(RuntimeEventScope::Evolution, RECOVERED_KIND)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn r1_event_replay_non_observation_dead_letter_keeps_existing_governance() {
        let fixture = ObservationWorkerFixture::new(false, "informational", 0, None, false);
        let source = fixture
            .events
            .append(RuntimeEventInput {
                stream_id: "session:worker-session".into(),
                scope: RuntimeEventScope::Tool,
                kind: "tool.invocation.failed".into(),
                status: Some("failed".into()),
                actor: Some("fixture.tool".into()),
                refs: vec![],
                payload: serde_json::json!({}),
            })
            .unwrap();
        fixture.projector().run_once(128).unwrap();
        assert!(fixture.projector().cursor().unwrap() >= source.commit_cursor);
        fixture.assert_counts(1, 0, 1, 0);
        let failure = fixture
            .events
            .event_by_idempotency_key(
                PROJECTOR_STREAM,
                &format!("dead-letter:{}", source.event_id),
            )
            .unwrap()
            .unwrap();
        assert_eq!(failure.payload["source_kind"], "tool.invocation.failed");
    }

    use harness_contract::{
        outcome::{
            OutcomeIdentity, OutcomeObservation, OutcomeQuality, OutcomeTiming, OutcomeUsage,
            RuntimeIdentity, StrategyIdentity, OUTCOME_SCHEMA_REVISION,
        },
        reality::EvidenceCompleteness,
        strategy::ExecutionCandidateKind,
    };

    #[test]
    fn r1_event_replay_observation_ref_preserves_evolution_classification() {
        let root = std::path::PathBuf::from(
            std::env::var_os("COWD_R1_06_FIXTURE_ROOT")
                .expect("explicit private plan fixture root"),
        )
        .join(format!("evolution-ref-{}", uuid::Uuid::new_v4()));
        assert!(
            root.is_absolute()
                && root
                    .starts_with("/media/yi/Datas/workspace/plan/2026-10/1016-cowd-future-kernel")
        );
        let events = Arc::new(RuntimeEventStore::for_test());
        events
            .bind_artifact_store(Arc::new(crate::ArtifactStore::for_test_default(root)))
            .unwrap();
        let discovery = Arc::new(EvolutionDiscoveryService::new(events.clone()));
        let projector = EvolutionSignalProjector::new(events.clone(), discovery.clone());
        for (index, result, gain) in [(0, "failed", 0), (1, "succeeded", 0), (2, "succeeded", 1)] {
            let observation: harness_contract::goal::RuntimeObservation = serde_json::from_value(serde_json::json!({
                "identity":{"workspace_id":"workspace","session_id":"ref-session","graph_id":"ref-graph","goal_id":"ref-goal"},
                "kind":"tool_progress","source":"current-source","source_revision":index+1,"freshness":{"observed_at_ms":index+1,"policy_revision":"accepted-current"},
                "summary":"private large body ".repeat(2048),"fingerprint":format!("current-{index}"),"result_class":result,
                "failure_class":if result == "failed" {Some(harness_contract::goal::ObservationFailureClass::Tool)} else {None},
                "cost_delta":harness_contract::goal::CostDelta {tool_calls:3,..Default::default()},"information_gain":harness_contract::goal::InformationGain {distinguishing_evidence_refs:if gain > 0 {vec!["observed:current".into()]} else {Vec::new()},resolved_unknown_refs:Vec::new(),provenance:harness_contract::core::MeasureProvenance::Observed}
            })).unwrap();
            let value = serde_json::to_value(observation).unwrap();
            let classifier = RuntimeEventStore::classify_observation(&value).unwrap();
            let body = events
                .prepare_content(
                    "session:ref-session",
                    &format!("observation-{index}"),
                    &value,
                    &crate::artifact::PreparedArtifactOrigin::EventBody {
                        stream_id: "goal:ref-goal".into(),
                        idempotency_key: format!("observation-{index}"),
                        kind: "goal.observation".into(),
                    },
                )
                .unwrap();
            assert_eq!(body["body"]["kind"], "artifact_ref");
            events.append_batch_if_revision("goal:ref-goal",index,format!("observation-{index}"),vec![RuntimeTransactionEventInput {
                event:RuntimeEventInput {stream_id:"goal:ref-goal".into(),scope:RuntimeEventScope::Goal,kind:"goal.observation".into(),status:None,actor:Some("runtime.goal_store".into()),refs:vec![RuntimeEventRef {kind:"session".into(),id:"ref-session".into()}],
                    payload:serde_json::json!({"observation_schema":2,"session_id":"ref-session","goal_source":{"stream_id":"goal:ref-goal","sequence":index},"classifier":classifier,"observation_body":body})},
                idempotency_key:Some(format!("observation-{index}")),schema_version:2,
            }]).unwrap();
            let event = events.latest_for_stream("goal:ref-goal").unwrap().unwrap();
            let expected = DurableRuntimeEvent {
                payload: serde_json::json!({"observation":value}),
                ..event.clone()
            };
            assert_eq!(
                projector.project_source(&event).unwrap(),
                classify_source_event(&expected).is_some()
            );
            assert_eq!(events.resolve_observation_record(&event).unwrap(), value);
            assert!(projector.project_source(&event).is_ok());
            let mut corrupt = event;
            corrupt.payload["classifier"]["tool_calls"] = serde_json::json!(999);
            assert!(projector.project_source(&corrupt).is_err());
        }
        let signals = discovery.list_signals().unwrap();
        assert_eq!(signals.len(), 2);
        assert!(signals
            .iter()
            .all(|signal| !signal.summary.contains("private large body")));
    }

    #[test]
    fn tool_experience_is_runtime_bound_private_preview_free_and_replay_safe() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&store)));
        let projector = EvolutionSignalProjector::new(Arc::clone(&store), Arc::clone(&discovery))
            .with_workspace_identity("workspace-tools".into());
        let invocation = crate::ToolInvocationRecord::started(
            "tool-session",
            1,
            "tool-call",
            "read_file",
            "PRIVATE_INPUT",
            crate::ToolSafetyCategory::ReadOnly,
            100,
        )
        .failed(
            crate::ToolFailureKind::ExecutionError,
            "PRIVATE_OUTPUT",
            150,
        );
        let binding: harness_contract::projection::RuntimeActivityBinding=serde_json::from_value(serde_json::json!({
            "root_execution_id":"root-tools","session_id":"tool-session","turn_id":"turn-tools",
            "root_task_id":"task-tools","task_id":"task-tools","activity_id":"activity-tools",
            "parent_activity_id":"activity-root-tools",
            "agent_instance_id":"agent-tools","tool_contract_id":"read_file","tool_call_id":"tool-call",
            "revision":1,"fence":1,"generation":1
        })).unwrap();
        let input = RuntimeEventInput {
            stream_id: "session:tool-session".into(),
            scope: RuntimeEventScope::Tool,
            kind: "tool.invocation.failed".into(),
            status: Some("failed".into()),
            actor: Some("conversation_runtime".into()),
            refs: vec![],
            payload: serde_json::to_value(&invocation).unwrap(),
        }
        .with_activity_binding(binding.clone())
        .unwrap();
        let event = store.append(input).unwrap();
        projector.run_once(128).unwrap();
        let signals = discovery.list_signals().unwrap();
        assert_eq!(signals.len(), 1);
        assert_eq!(
            signals[0].source.session_id.as_deref(),
            Some("tool-session")
        );
        assert_eq!(signals[0].scope.workspace_identity, "workspace-tools");
        let encoded = serde_json::to_string(&signals).unwrap();
        assert!(!encoded.contains("PRIVATE_INPUT") && !encoded.contains("PRIVATE_OUTPUT"));
        assert_eq!(discovery.list_cases(10).unwrap().len(), 1);
        let reopened = EvolutionSignalProjector::new(Arc::clone(&store), Arc::clone(&discovery))
            .with_workspace_identity("workspace-tools".into());
        reopened.project_source(&event).unwrap();
        reopened.run_once(128).unwrap();
        assert_eq!(discovery.list_signals().unwrap().len(), 1);
        assert_eq!(discovery.list_cases(10).unwrap().len(), 1);
        let mut wrong = event.clone();
        wrong.payload["session_id"] = serde_json::json!("foreign-session");
        assert!(reopened.tool_experience_signal(&wrong).is_err());
        assert!(
            EvolutionSignalProjector::new(Arc::clone(&store), Arc::clone(&discovery))
                .tool_experience_signal(&event)
                .is_err()
        );
        let fast = invocation.started_fact().completed("private success", 110);
        let mut success = event.clone();
        success.kind = "tool.invocation.completed".into();
        success.payload = serde_json::to_value(fast).unwrap();
        success.payload["_runtime_activity_binding"] = serde_json::to_value(binding).unwrap();
        assert!(reopened.tool_experience_signal(&success).unwrap().is_none());
        let slow = invocation
            .started_fact()
            .completed("PRIVATE_SLOW_OUTPUT", 31_000);
        success.payload["duration_ms"] = serde_json::json!(slow.duration_ms);
        success.payload["ended_at_ms"] = serde_json::json!(slow.ended_at_ms);
        assert_eq!(
            reopened
                .tool_experience_signal(&success)
                .unwrap()
                .unwrap()
                .signal_type,
            EvolutionSignalType::SlowProgress
        );
    }

    fn failed_agent_evaluation(index: u64) -> crate::AgentRunEvaluation {
        crate::AgentRunEvaluation {
            schema_revision: 1,
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
            required_acceptance: harness_contract::context::RequiredAcceptance {
                criteria: vec!["task_success".to_string()],
                evidence_obligations: Vec::new(),
            },
            observed_acceptance: harness_contract::context::ObservedAcceptance::default(),
            acceptance_evaluation: None,
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
                execution_scope: harness_contract::outcome::OutcomeExecutionScope::Task,
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
                build: Default::default(),
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
        let events = Arc::new(RuntimeEventStore::for_test());
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

    fn assert_regression_within(
        label: &str,
        baseline: u128,
        projected: u128,
        limit_percent: u128,
        absolute_noise_us: u128,
    ) {
        let within_relative = projected.saturating_mul(100)
            <= baseline.saturating_mul(100_u128.saturating_add(limit_percent));
        let within_absolute = projected.saturating_sub(baseline) <= absolute_noise_us;
        assert!(
            within_relative || within_absolute,
            "{label} regression exceeded {limit_percent}% and {absolute_noise_us}us: baseline={baseline}, projected={projected}"
        );
    }

    fn baseline_probe(backlog: usize, samples: usize, prefix: &str) -> (Duration, Vec<u128>) {
        let events = RuntimeEventStore::for_test();
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
        let events = Arc::new(RuntimeEventStore::for_test());
        if backlog > 0 {
            append_foreground_probe(&events, &format!("{prefix}-backlog"), backlog);
        }
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = Arc::new(EvolutionSignalProjector::new(
            Arc::clone(&events),
            discovery,
        ));
        let reactor = Arc::new(
            crate::RuntimeEventReactor::sealed(
                Arc::clone(&events),
                [projector.projection_lane().unwrap()],
            )
            .unwrap(),
        );
        reactor.start().unwrap();
        // Start the paired foreground measurement only after the maintenance
        // lane has entered its durable catch-up pass.  Otherwise the first
        // scheduler/bootstrap race is charged to the foreground sample while
        // later rounds are measured against an already-active lane.  The
        // remaining backlog and the foreground events keep this a real
        // contention measurement rather than a warmed-idle projection.
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
        let observation = append_foreground_probe(&events, prefix, samples);
        if backlog > 0 {
            assert!(
                projector.health().expect("projector health").lag_commits > 0,
                "foreground measurement must overlap durable catch-up"
            );
        }
        let report = reactor.shutdown().await;
        assert!(report.timed_out_lanes.is_empty());
        observation
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "run scripts/test/runtime-projection-performance.sh for paired foreground evidence"]
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
            5_000,
        );
        assert_regression_within(
            "foreground p95",
            baseline.p95_us(),
            projected.p95_us(),
            3,
            10,
        );
        assert_regression_within(
            "foreground p99",
            baseline.p99_us(),
            projected.p99_us(),
            5,
            20,
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "run scripts/test/runtime-projection-performance.sh for active catchup evidence"]
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
            5_000,
        );
        assert_regression_within(
            "active-catchup foreground p95",
            baseline.p95_us(),
            projected.p95_us(),
            3,
            10,
        );
        assert_regression_within(
            "active-catchup foreground p99",
            baseline.p99_us(),
            projected.p99_us(),
            5,
            20,
        );
    }

    #[test]
    fn projector_replays_source_once_and_crosses_its_own_checkpoint() {
        let events = Arc::new(RuntimeEventStore::for_test());
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

        let observation: harness_contract::goal::RuntimeObservation = serde_json::from_value(serde_json::json!({
            "identity":{"workspace_id":"workspace","session_id":"session-2","graph_id":"graph-2","goal_id":"goal-2"},
            "kind":"tool_progress","source":"current-provider","source_revision":1,"freshness":{"observed_at_ms":1,"policy_revision":"accepted-current"},
            "summary":"typed failure","fingerprint":"failure-2","result_class":"failed"
        })).unwrap();
        let value = serde_json::to_value(observation).unwrap();
        let body = events
            .prepare_content(
                "session:session-2",
                "failure-2",
                &value,
                &crate::artifact::PreparedArtifactOrigin::EventBody {
                    stream_id: "goal:goal-2".into(),
                    idempotency_key: "failure-2".into(),
                    kind: "goal.observation".into(),
                },
            )
            .unwrap();
        events.append_batch_if_revision("goal:goal-2",0,"failure-2",vec![RuntimeTransactionEventInput {
            event:RuntimeEventInput {stream_id:"goal:goal-2".into(),scope:RuntimeEventScope::Goal,kind:"goal.observation".into(),status:Some("failed".into()),actor:Some("runtime.goal".into()),refs:Vec::new(),
                payload:serde_json::json!({"observation_schema":2,"session_id":"session-2","goal_source":{"stream_id":"goal:goal-2","sequence":0},"classifier":RuntimeEventStore::classify_observation(&value).unwrap(),"observation_body":body})},
            idempotency_key:Some("failure-2".into()),schema_version:2,
        }]).expect("second current-schema source event");
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
    fn projector_replay_keeps_existing_deterministic_signal_when_payload_evolved() {
        let events = Arc::new(RuntimeEventStore::for_test());
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = EvolutionSignalProjector::new(Arc::clone(&events), Arc::clone(&discovery));
        events
            .append(RuntimeEventInput {
                stream_id: "goal:historical-conflict".to_string(),
                scope: RuntimeEventScope::Goal,
                kind: "goal.intervention".to_string(),
                status: Some("replan".to_string()),
                actor: Some("runtime.intervention_policy".to_string()),
                refs: vec![RuntimeEventRef {
                    kind: "session".to_string(),
                    id: "session-historical-conflict".to_string(),
                }],
                payload: serde_json::json!({
                    "intervention": {"reason": "historical slow progress"}
                }),
            })
            .expect("historical source event");
        let source = events
            .list_stream("goal:historical-conflict")
            .expect("source stream")
            .into_iter()
            .next()
            .expect("source event");
        let mut historical = signal_from_source_event(&source).expect("projected signal");
        historical.summary = "summary emitted by an older projector revision".to_string();
        let historical = discovery
            .record_signal(historical)
            .expect("historical canonical signal");

        projector.run_once(64).expect("replay must not conflict");

        assert_eq!(
            discovery
                .signal(&historical.signal_id)
                .expect("canonical signal"),
            Some(historical)
        );
        assert_eq!(discovery.list_cases(25).expect("cases").len(), 1);
        let health = projector.health().expect("projector health");
        assert_eq!(health.dead_letter_count, 0);
        assert_eq!(health.lag_commits, 0);
    }

    #[test]
    fn projector_checkpoint_is_mutable_and_does_not_emit_a_commit() {
        let events = Arc::new(RuntimeEventStore::for_test());
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

        assert_eq!(projector.run_once(64).expect("projector pass"), 0);
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
        let events = Arc::new(RuntimeEventStore::for_test());
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
    fn malformed_legacy_signal_does_not_block_the_following_valid_signal() {
        let events = Arc::new(RuntimeEventStore::for_test());
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = EvolutionSignalProjector::new(Arc::clone(&events), Arc::clone(&discovery));
        events
            .append(RuntimeEventInput {
                stream_id: "evolution:signal:malformed-legacy".to_string(),
                scope: RuntimeEventScope::Evolution,
                kind: "evolution.signal.recorded.v1".to_string(),
                status: Some("warning".to_string()),
                actor: Some("legacy.runtime".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"obsolete": true}),
            })
            .expect("malformed legacy signal");
        let mut valid = EvolutionSignal::low_novelty_tool_loop(
            "legacy-runtime",
            "legacy-session",
            vec![EvidenceRef::observed("legacy", "valid-evidence")],
        );
        valid.signal_id = "valid-after-malformed".to_string();
        events
            .append(RuntimeEventInput {
                stream_id: "legacy:evolution:valid-after-malformed".to_string(),
                scope: RuntimeEventScope::Evolution,
                kind: "evolution.signal.recorded.v1".to_string(),
                status: Some("warning".to_string()),
                actor: Some("legacy.runtime".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"signal": valid}),
            })
            .expect("valid legacy signal");

        projector.run_once(64).expect("bounded bootstrap");
        assert!(discovery
            .signal("valid-after-malformed")
            .expect("canonical signal lookup")
            .is_some());
        let checkpoint = events
            .projection_checkpoint(LEGACY_BOOTSTRAP_ID)
            .expect("checkpoint read")
            .expect("bootstrap advanced");
        assert!(checkpoint.source_cursor > 0);
    }

    #[test]
    fn canonical_outcome_enters_governed_evolution_without_direct_promotion() {
        let events = Arc::new(RuntimeEventStore::for_test());
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
        let events = Arc::new(RuntimeEventStore::for_test());
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
