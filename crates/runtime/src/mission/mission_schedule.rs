//! Durable Mission schedule projection and timer event source.
//!
//! The store decides which schedule fires are due and records their stable
//! correlation IDs. It does not execute a node or a turn: RuntimeServices
//! compiles every claimed fire into a canonical SessionDispatch graph.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use harness_contract::mission::{
    MissionSchedule, MissionScheduleFire, MissionScheduleFireHistoryCursor,
    MissionScheduleFireHistoryPage, MissionScheduleFireStatus, MissionScheduleStatus,
    ScheduleTrigger,
};
use harness_contract::policy::{ExecutionPolicyBinding, PermissionMode};
use serde::{Deserialize, Serialize};

use crate::runtime_event_store::RuntimeTransactionEventInput;
use crate::{RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore};

const FIRE_RECEIPT_EVENT_KIND: &str = "mission.schedule.fire_receipt.v2";
// RuntimeEventStore admits at most 10_000 events per transaction. Reserve one
// slot for the active-only snapshot that must be committed last.
const MAX_TERMINAL_RECEIPTS_PER_COMMIT: usize = 9_999;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateMissionScheduleRequest {
    pub mission_id: String,
    pub target_session_id: String,
    pub objective: String,
    pub trigger: ScheduleTrigger,
    pub permission_ceiling: PermissionMode,
    pub priority: u8,
}

/// Partial, revision-checked schedule mutation. The timer may observe a
/// schedule while an operator edits it, so every update is rejected when its
/// caller did not base the change on the current durable revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateMissionScheduleRequest {
    pub expected_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger: Option<ScheduleTrigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionScheduleTickReport {
    pub kind: String,
    pub now_ms: u64,
    pub claimed: Vec<MissionScheduleFire>,
    pub missed: Vec<MissionScheduleFire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionScheduleDispatchReport {
    pub kind: String,
    pub tick: MissionScheduleTickReport,
    pub submitted: Vec<MissionScheduleFire>,
    pub failed: Vec<MissionScheduleFire>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MissionScheduleFireTerminal {
    Completed { fire_id: String },
    Failed { fire_id: String, error: String },
    Cancelled { fire_id: String, reason: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct MissionScheduleState {
    schedules: BTreeMap<String, MissionSchedule>,
    fires: BTreeMap<String, MissionScheduleFire>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MissionScheduleStateEvent {
    state: MissionScheduleState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MissionScheduleFireReceiptEvent {
    fire: MissionScheduleFire,
}

/// Event-sourced cache. It can be discarded and recreated from one stream.
pub struct MissionScheduleStore {
    event_store: Arc<RuntimeEventStore>,
    stream_id: String,
    state: Mutex<MissionScheduleState>,
}

impl MissionScheduleStore {
    pub fn event_sourced(
        event_store: Arc<RuntimeEventStore>,
        workspace_key: impl Into<String>,
    ) -> Result<Self, String> {
        let stream_id = format!("mission-schedules:{}", workspace_key.into());
        Ok(Self {
            state: Mutex::new(load_state(&event_store, &stream_id)?),
            event_store,
            stream_id,
        })
    }

    pub fn create(
        &self,
        request: CreateMissionScheduleRequest,
        now_ms: u64,
    ) -> Result<MissionSchedule, String> {
        validate_create_request(&request)?;
        let schedule_id = format!("mission-schedule-{}", uuid::Uuid::new_v4());
        let next_at_ms = next_after(&request.trigger, now_ms.saturating_sub(1))?
            .ok_or_else(|| "schedule trigger has no next occurrence".to_string())?;
        let schedule = MissionSchedule {
            schedule_id: schedule_id.clone(),
            mission_id: request.mission_id,
            target_session_id: request.target_session_id,
            objective: request.objective,
            trigger: request.trigger,
            permission_ceiling: request.permission_ceiling,
            priority: request.priority,
            next_at_ms,
            status: MissionScheduleStatus::Scheduled,
            revision: 1,
        };
        self.mutate("mission.schedule.created.v1", |state| {
            state.schedules.insert(schedule_id, schedule.clone());
            Ok(schedule.clone())
        })
    }

    pub fn pause(&self, schedule_id: &str) -> Result<MissionSchedule, String> {
        self.update_schedule(schedule_id, "mission.schedule.paused.v1", |schedule| {
            schedule.status = MissionScheduleStatus::Paused;
            Ok(())
        })
    }

    pub fn update(
        &self,
        schedule_id: &str,
        request: UpdateMissionScheduleRequest,
        now_ms: u64,
    ) -> Result<MissionSchedule, String> {
        self.update_schedule(schedule_id, "mission.schedule.updated.v1", |schedule| {
            if schedule.revision != request.expected_revision {
                return Err(format!(
                    "schedule revision stale: expected {}, actual {}",
                    request.expected_revision, schedule.revision
                ));
            }
            if let Some(objective) = request.objective {
                if objective.trim().is_empty() {
                    return Err("schedule objective must not be empty".to_string());
                }
                schedule.objective = objective;
            }
            if let Some(trigger) = request.trigger {
                validate_trigger(&trigger)?;
                schedule.trigger = trigger;
                schedule.next_at_ms = next_after(&schedule.trigger, now_ms)?
                    .ok_or_else(|| "schedule trigger has no next occurrence".to_string())?;
            }
            if let Some(priority) = request.priority {
                schedule.priority = priority;
            }
            Ok(())
        })
    }

    pub fn resume(&self, schedule_id: &str, now_ms: u64) -> Result<MissionSchedule, String> {
        self.update_schedule(schedule_id, "mission.schedule.resumed.v1", |schedule| {
            if schedule.status == MissionScheduleStatus::Completed {
                return Err("one-shot schedule is completed and cannot resume".to_string());
            }
            schedule.status = MissionScheduleStatus::Scheduled;
            if schedule.next_at_ms < now_ms {
                schedule.next_at_ms = next_after(&schedule.trigger, now_ms.saturating_sub(1))?
                    .ok_or_else(|| "schedule trigger has no next occurrence".to_string())?;
            }
            Ok(())
        })
    }

    /// Creates a durable one-off fire without moving the next automatic
    /// occurrence. Manual and timer runs share one dispatch and evidence path.
    pub fn trigger_now(
        &self,
        schedule_id: &str,
        now_ms: u64,
    ) -> Result<MissionScheduleFire, String> {
        self.mutate("mission.schedule.triggered_manually.v1", |state| {
            let schedule = state
                .schedules
                .get(schedule_id)
                .ok_or_else(|| format!("mission schedule not found: {schedule_id}"))?;
            let fire_id = format!("{}:manual:{}", schedule.schedule_id, uuid::Uuid::new_v4());
            let fire = MissionScheduleFire {
                correlation_id: format!("schedule-fire:{fire_id}"),
                fire_id: fire_id.clone(),
                schedule_id: schedule.schedule_id.clone(),
                mission_id: schedule.mission_id.clone(),
                due_at_ms: now_ms,
                target_session_id: schedule.target_session_id.clone(),
                objective: schedule.objective.clone(),
                permission_ceiling: schedule.permission_ceiling.clone(),
                target_policy_binding: None,
                priority: schedule.priority,
                status: MissionScheduleFireStatus::Pending,
                graph_id: None,
                error: None,
            };
            state.fires.insert(fire_id, fire.clone());
            Ok(fire)
        })
    }

    /// Removes future scheduling while retaining historical fire receipts as
    /// immutable evidence of work already attempted or submitted.
    pub fn delete(&self, schedule_id: &str) -> Result<MissionSchedule, String> {
        self.mutate("mission.schedule.deleted.v1", |state| {
            state
                .schedules
                .remove(schedule_id)
                .ok_or_else(|| format!("mission schedule not found: {schedule_id}"))
        })
    }

    /// Claims due schedule occurrences. A claimed occurrence is durable before
    /// GraphRunner sees it, so restart recovery may submit the same stable
    /// graph idempotency key without running it twice.
    pub fn claim_due(
        &self,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<MissionScheduleTickReport, String> {
        self.mutate("mission.schedule.fired_or_missed.v1", |state| {
            let mut claimed = Vec::new();
            let mut missed = Vec::new();
            let ids = state.schedules.keys().cloned().collect::<Vec<_>>();
            for schedule_id in ids {
                let Some(schedule) = state.schedules.get_mut(&schedule_id) else {
                    continue;
                };
                if schedule.status != MissionScheduleStatus::Scheduled
                    || schedule.next_at_ms > now_ms
                {
                    continue;
                }
                let due_at_ms = schedule.next_at_ms;
                let fire_id = format!("{}:{due_at_ms}", schedule.schedule_id);
                if state.fires.contains_key(&fire_id) {
                    continue;
                }
                let late_by = now_ms.saturating_sub(due_at_ms);
                let status = if late_by > grace_ms {
                    MissionScheduleFireStatus::Missed
                } else {
                    MissionScheduleFireStatus::Pending
                };
                let fire = MissionScheduleFire {
                    correlation_id: format!("schedule-fire:{fire_id}"),
                    fire_id: fire_id.clone(),
                    schedule_id: schedule.schedule_id.clone(),
                    mission_id: schedule.mission_id.clone(),
                    due_at_ms,
                    target_session_id: schedule.target_session_id.clone(),
                    objective: schedule.objective.clone(),
                    permission_ceiling: schedule.permission_ceiling.clone(),
                    target_policy_binding: None,
                    priority: schedule.priority,
                    status,
                    graph_id: None,
                    error: (status == MissionScheduleFireStatus::Missed).then(|| {
                        format!("schedule occurrence exceeded grace window by {late_by}ms")
                    }),
                };
                state.fires.insert(fire_id, fire.clone());
                match next_after(&schedule.trigger, due_at_ms)? {
                    Some(next) => {
                        schedule.next_at_ms = next;
                        schedule.revision = schedule.revision.saturating_add(1);
                    }
                    None => {
                        schedule.status = MissionScheduleStatus::Completed;
                        schedule.revision = schedule.revision.saturating_add(1);
                    }
                }
                if status == MissionScheduleFireStatus::Missed {
                    missed.push(fire);
                } else {
                    claimed.push(fire);
                }
            }
            Ok(MissionScheduleTickReport {
                kind: "runtime.mission_schedule_tick".to_string(),
                now_ms,
                claimed,
                missed,
            })
        })
    }

    /// Captures the target Session's effective policy exactly once before Graph
    /// submission. Replaying the same binding is idempotent; attempting to
    /// substitute a different policy snapshot fails closed.
    pub fn bind_target_policy(
        &self,
        fire_id: &str,
        binding: ExecutionPolicyBinding,
    ) -> Result<MissionScheduleFire, String> {
        binding.validate().map_err(str::to_string)?;
        self.mutate("mission.schedule.policy_bound.v1", |state| {
            let fire = state
                .fires
                .get_mut(fire_id)
                .ok_or_else(|| format!("active schedule fire not found: {fire_id}"))?;
            if fire.status != MissionScheduleFireStatus::Pending {
                return Err(format!(
                    "schedule fire {fire_id} cannot bind policy from {}",
                    fire.status.as_str()
                ));
            }
            if binding.session_id != fire.target_session_id {
                return Err("schedule fire policy binding targets another Session".to_string());
            }
            if binding.permission_ceiling != fire.permission_ceiling {
                return Err(
                    "schedule fire policy binding ceiling does not match schedule".to_string(),
                );
            }
            match &fire.target_policy_binding {
                Some(existing) if existing == &binding => return Ok(fire.clone()),
                Some(_) => {
                    return Err(
                        "schedule fire already has a different effective policy binding"
                            .to_string(),
                    );
                }
                None => fire.target_policy_binding = Some(binding),
            }
            Ok(fire.clone())
        })
    }

    pub fn mark_submitted(
        &self,
        fire_id: &str,
        graph_id: String,
    ) -> Result<MissionScheduleFire, String> {
        if let Some(existing) = self.fire_by_id(fire_id)? {
            if existing.status == MissionScheduleFireStatus::Submitted {
                return (existing.graph_id.as_deref() == Some(graph_id.as_str()))
                    .then_some(existing)
                    .ok_or_else(|| {
                        format!("schedule fire {fire_id} already submitted to another graph")
                    });
            }
            if is_terminal_fire(&existing) {
                return Err(format!(
                    "schedule fire {fire_id} cannot submit from {}",
                    existing.status.as_str()
                ));
            }
        }
        self.mutate("mission.schedule.submitted.v1", |state| {
            let fire = state
                .fires
                .get_mut(fire_id)
                .ok_or_else(|| format!("schedule fire not found: {fire_id}"))?;
            if fire.status != MissionScheduleFireStatus::Pending
                && fire.status != MissionScheduleFireStatus::Submitted
            {
                return Err(format!(
                    "schedule fire {fire_id} cannot submit from {}",
                    fire.status.as_str()
                ));
            }
            if fire.target_policy_binding.is_none() {
                return Err(format!(
                    "schedule fire {fire_id} has no effective target policy binding"
                ));
            }
            fire.status = MissionScheduleFireStatus::Submitted;
            fire.graph_id = Some(graph_id);
            Ok(fire.clone())
        })
    }

    pub fn mark_completed(&self, fire_id: &str) -> Result<MissionScheduleFire, String> {
        if let Some(existing) = self.fire_by_id(fire_id)? {
            if existing.status == MissionScheduleFireStatus::Completed {
                return Ok(existing);
            }
            if is_terminal_fire(&existing) {
                return Err(format!(
                    "schedule fire {fire_id} cannot complete from {}",
                    existing.status.as_str()
                ));
            }
        }
        self.mutate("mission.schedule.completed.v1", |state| {
            let fire = state
                .fires
                .get_mut(fire_id)
                .ok_or_else(|| format!("active schedule fire not found: {fire_id}"))?;
            if fire.status != MissionScheduleFireStatus::Submitted {
                return Err(format!(
                    "schedule fire {fire_id} cannot complete from {}",
                    fire.status.as_str()
                ));
            }
            fire.status = MissionScheduleFireStatus::Completed;
            Ok(fire.clone())
        })
    }

    pub fn mark_cancelled(
        &self,
        fire_id: &str,
        reason: String,
    ) -> Result<MissionScheduleFire, String> {
        if let Some(existing) = self.fire_by_id(fire_id)? {
            if existing.status == MissionScheduleFireStatus::Cancelled {
                return Ok(existing);
            }
            if is_terminal_fire(&existing) {
                return Err(format!(
                    "schedule fire {fire_id} cannot cancel from {}",
                    existing.status.as_str()
                ));
            }
        }
        self.mutate("mission.schedule.cancelled.v1", |state| {
            let fire = state
                .fires
                .get_mut(fire_id)
                .ok_or_else(|| format!("active schedule fire not found: {fire_id}"))?;
            fire.status = MissionScheduleFireStatus::Cancelled;
            fire.error = (!reason.trim().is_empty()).then_some(reason);
            Ok(fire.clone())
        })
    }

    /// Applies one reconciliation batch under a single event-store transaction
    /// and emits one active-only snapshot. This keeps a 10k terminal wave O(n)
    /// receipts + O(active-after), rather than serializing n shrinking copies.
    pub fn mark_terminal_batch(
        &self,
        terminals: Vec<MissionScheduleFireTerminal>,
    ) -> Result<Vec<MissionScheduleFire>, String> {
        if terminals.is_empty() {
            return Ok(Vec::new());
        }
        let mut receipts = Vec::with_capacity(terminals.len());
        for chunk in terminals.chunks(MAX_TERMINAL_RECEIPTS_PER_COMMIT) {
            let mut committed = self.mutate("mission.schedule.terminal_batch.v1", |state| {
                apply_terminal_batch(state, chunk.to_vec())
            })?;
            receipts.append(&mut committed);
        }
        Ok(receipts)
    }

    pub fn mark_failed(&self, fire_id: &str, error: String) -> Result<MissionScheduleFire, String> {
        if let Some(existing) = self.fire_by_id(fire_id)? {
            if existing.status == MissionScheduleFireStatus::Failed {
                return (existing.error.as_deref() == Some(error.as_str()))
                    .then_some(existing)
                    .ok_or_else(|| {
                        format!("schedule fire {fire_id} already failed with another error")
                    });
            }
            if is_terminal_fire(&existing) {
                return Err(format!(
                    "schedule fire {fire_id} cannot fail from {}",
                    existing.status.as_str()
                ));
            }
        }
        self.mutate("mission.schedule.failed.v1", |state| {
            let fire = state
                .fires
                .get_mut(fire_id)
                .ok_or_else(|| format!("schedule fire not found: {fire_id}"))?;
            fire.status = MissionScheduleFireStatus::Failed;
            fire.error = Some(error);
            Ok(fire.clone())
        })
    }

    /// Returns the latest durable receipt for one fire. Terminal receipts do
    /// not remain in the hot schedule projection.
    pub fn fire_by_id(&self, fire_id: &str) -> Result<Option<MissionScheduleFire>, String> {
        if let Some(active) = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fires
            .get(fire_id)
            .cloned()
        {
            return Ok(Some(active));
        }
        let mut offset = 0;
        const PAGE: usize = 256;
        loop {
            let events = self
                .event_store
                .list_stream_page_desc(&self.stream_id, PAGE, offset)?;
            if events.is_empty() {
                return Ok(None);
            }
            let event_count = events.len();
            for event in events {
                if event.kind != FIRE_RECEIPT_EVENT_KIND {
                    continue;
                }
                let receipt =
                    serde_json::from_value::<MissionScheduleFireReceiptEvent>(event.payload)
                        .map_err(|error| error.to_string())?;
                if receipt.fire.fire_id == fire_id {
                    return Ok(Some(receipt.fire));
                }
            }
            if event_count < PAGE {
                return Ok(None);
            }
            offset = offset.saturating_add(event_count);
        }
    }

    /// Cursor-paginated durable fire history. The hot projection intentionally
    /// contains active fires only; operators can page this evidence without an
    /// O(history) rebuild or snapshot.
    pub fn fire_history_page(
        &self,
        after: Option<MissionScheduleFireHistoryCursor>,
        limit: usize,
    ) -> Result<MissionScheduleFireHistoryPage, String> {
        let limit = limit.clamp(1, 500);
        let mut cursor = after.map(|cursor| (cursor.commit_cursor, cursor.transaction_index));
        let mut matches = Vec::with_capacity(limit.saturating_add(1));
        const SOURCE_PAGE: usize = 512;
        while matches.len() <= limit {
            let page = self.event_store.list_scope_stream_prefix_page_asc(
                RuntimeEventScope::Mission,
                &self.stream_id,
                cursor,
                SOURCE_PAGE,
            )?;
            if page.is_empty() {
                break;
            }
            let page_len = page.len();
            for event in page {
                cursor = Some((event.commit_cursor, event.transaction_index));
                if event.stream_id != self.stream_id || event.kind != FIRE_RECEIPT_EVENT_KIND {
                    continue;
                }
                let receipt =
                    serde_json::from_value::<MissionScheduleFireReceiptEvent>(event.payload)
                        .map_err(|error| error.to_string())?;
                matches.push((
                    MissionScheduleFireHistoryCursor {
                        commit_cursor: event.commit_cursor,
                        transaction_index: event.transaction_index,
                    },
                    receipt.fire,
                ));
                if matches.len() > limit {
                    break;
                }
            }
            if matches.len() > limit || page_len < SOURCE_PAGE {
                break;
            }
        }
        let has_more = matches.len() > limit;
        if has_more {
            matches.pop();
        }
        let next_cursor = matches.last().map(|(cursor, _)| *cursor);
        Ok(MissionScheduleFireHistoryPage {
            fires: matches.into_iter().map(|(_, fire)| fire).collect(),
            next_cursor,
            has_more,
        })
    }

    #[must_use]
    pub fn active_fire_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fires
            .len()
    }

    #[must_use]
    pub fn pending_fires(&self) -> Vec<MissionScheduleFire> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fires
            .values()
            .filter(|fire| fire.status == MissionScheduleFireStatus::Pending)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn submitted_fires(&self) -> Vec<MissionScheduleFire> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fires
            .values()
            .filter(|fire| fire.status == MissionScheduleFireStatus::Submitted)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn projection(&self) -> serde_json::Value {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        serde_json::json!({
            "kind": "runtime.mission_schedules",
            "stream_id": self.stream_id,
            "schedules": state.schedules.values().cloned().collect::<Vec<_>>(),
            "fires": state.fires.values().cloned().collect::<Vec<_>>(),
            "fire_history": "durable_paginated",
        })
    }

    fn update_schedule(
        &self,
        schedule_id: &str,
        event_kind: &str,
        update: impl FnOnce(&mut MissionSchedule) -> Result<(), String>,
    ) -> Result<MissionSchedule, String> {
        self.mutate(event_kind, |state| {
            let schedule = state
                .schedules
                .get_mut(schedule_id)
                .ok_or_else(|| format!("mission schedule not found: {schedule_id}"))?;
            update(schedule)?;
            schedule.revision = schedule.revision.saturating_add(1);
            Ok(schedule.clone())
        })
    }

    fn mutate<T>(
        &self,
        event_kind: &str,
        operation: impl FnOnce(&mut MissionScheduleState) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = state.clone();
        let result = operation(&mut state)?;
        let fire_receipts = changed_fire_receipts(&previous, &state);
        let active_state = active_only_state(&state);
        let previous_active = active_only_state(&previous);
        // A timer tick that finds no due schedule is an observation, not a
        // domain mutation. Persisting a complete snapshot for every empty
        // tick bloats the event stream and can contend with foreground turn
        // execution. Only durable state transitions receive an event.
        if active_state == previous_active && fire_receipts.is_empty() {
            return Ok(result);
        }
        if let Err(error) = self.commit(&active_state, &fire_receipts, event_kind) {
            *state = previous;
            return Err(error);
        }
        *state = active_state;
        Ok(result)
    }

    fn commit(
        &self,
        state: &MissionScheduleState,
        fire_receipts: &[MissionScheduleFire],
        event_kind: &str,
    ) -> Result<(), String> {
        let revision = self
            .event_store
            .stream_revision(&self.stream_id)
            .map_err(|error| error.to_string())?;
        let transaction_id = format!(
            "mission-schedules:{}:{}",
            self.stream_id,
            revision.saturating_add(1)
        );
        let mut events = fire_receipts
            .iter()
            .map(|fire| {
                Ok(RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: self.stream_id.clone(),
                        scope: RuntimeEventScope::Mission,
                        kind: FIRE_RECEIPT_EVENT_KIND.to_string(),
                        status: Some(fire.status.as_str().to_string()),
                        actor: Some("mission_schedule_store".to_string()),
                        refs: vec![
                            RuntimeEventRef {
                                kind: "schedule".to_string(),
                                id: fire.schedule_id.clone(),
                            },
                            RuntimeEventRef {
                                kind: "fire".to_string(),
                                id: fire.fire_id.clone(),
                            },
                        ],
                        payload: serde_json::to_value(MissionScheduleFireReceiptEvent {
                            fire: fire.clone(),
                        })
                        .map_err(|error| error.to_string())?,
                    },
                    idempotency_key: Some(format!("mission-fire:{event_kind}:{}", fire.fire_id)),
                    schema_version: 2,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        // The hot-state snapshot is deliberately last: latest_for_stream can
        // rebuild in O(1), while preceding fire receipts retain full history.
        events.push(RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: self.stream_id.clone(),
                scope: RuntimeEventScope::Mission,
                kind: event_kind.to_string(),
                status: Some("committed".to_string()),
                actor: Some("mission_schedule_store".to_string()),
                refs: Vec::new(),
                payload: serde_json::to_value(MissionScheduleStateEvent {
                    state: state.clone(),
                })
                .map_err(|error| error.to_string())?,
            },
            idempotency_key: Some(format!(
                "schedule-hot-state:{}:{}",
                revision.saturating_add(1),
                fire_receipts.len()
            )),
            schema_version: 2,
        });
        self.event_store
            .append_batch_if_revision(self.stream_id.clone(), revision, transaction_id, events)
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

fn is_terminal_fire(fire: &MissionScheduleFire) -> bool {
    fire.status.is_terminal()
}

fn active_only_state(state: &MissionScheduleState) -> MissionScheduleState {
    MissionScheduleState {
        schedules: state.schedules.clone(),
        fires: state
            .fires
            .iter()
            .filter(|(_, fire)| !is_terminal_fire(fire))
            .map(|(id, fire)| (id.clone(), fire.clone()))
            .collect(),
    }
}

fn changed_fire_receipts(
    previous: &MissionScheduleState,
    current: &MissionScheduleState,
) -> Vec<MissionScheduleFire> {
    current
        .fires
        .iter()
        .filter(|(id, fire)| previous.fires.get(*id) != Some(*fire))
        .map(|(_, fire)| fire.clone())
        .collect()
}

fn apply_terminal_batch(
    state: &mut MissionScheduleState,
    terminals: Vec<MissionScheduleFireTerminal>,
) -> Result<Vec<MissionScheduleFire>, String> {
    let mut completed = Vec::with_capacity(terminals.len());
    for terminal in terminals {
        let (fire_id, status, detail) = match terminal {
            MissionScheduleFireTerminal::Completed { fire_id } => {
                (fire_id, MissionScheduleFireStatus::Completed, None)
            }
            MissionScheduleFireTerminal::Failed { fire_id, error } => {
                (fire_id, MissionScheduleFireStatus::Failed, Some(error))
            }
            MissionScheduleFireTerminal::Cancelled { fire_id, reason } => (
                fire_id,
                MissionScheduleFireStatus::Cancelled,
                (!reason.trim().is_empty()).then_some(reason),
            ),
        };
        let fire = state
            .fires
            .get_mut(&fire_id)
            .ok_or_else(|| format!("active schedule fire not found: {fire_id}"))?;
        let transition_allowed = fire.status == MissionScheduleFireStatus::Submitted
            || (status == MissionScheduleFireStatus::Failed
                && fire.status == MissionScheduleFireStatus::Pending);
        if !transition_allowed {
            return Err(format!(
                "schedule fire {fire_id} cannot become {} from {}",
                status.as_str(),
                fire.status.as_str()
            ));
        }
        fire.status = status;
        fire.error = detail;
        completed.push(fire.clone());
    }
    Ok(completed)
}

fn validate_create_request(request: &CreateMissionScheduleRequest) -> Result<(), String> {
    for (name, value) in [
        ("mission_id", request.mission_id.as_str()),
        ("target_session_id", request.target_session_id.as_str()),
        ("objective", request.objective.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("schedule {name} must not be empty"));
        }
    }
    validate_trigger(&request.trigger)
}

fn validate_trigger(trigger: &ScheduleTrigger) -> Result<(), String> {
    match trigger {
        ScheduleTrigger::At { .. } => Ok(()),
        ScheduleTrigger::Interval { every_ms } if *every_ms > 0 => Ok(()),
        ScheduleTrigger::Interval { .. } => Err("schedule interval must be positive".to_string()),
        ScheduleTrigger::Cron {
            expression,
            timezone,
        } => {
            Schedule::from_str(expression)
                .map_err(|error| format!("invalid cron expression: {error}"))?;
            timezone
                .parse::<Tz>()
                .map_err(|_| format!("invalid IANA timezone: {timezone}"))?;
            Ok(())
        }
    }
}

fn next_after(trigger: &ScheduleTrigger, after_ms: u64) -> Result<Option<u64>, String> {
    match trigger {
        ScheduleTrigger::At { at_ms } => Ok((*at_ms > after_ms).then_some(*at_ms)),
        ScheduleTrigger::Interval { every_ms } => {
            if *every_ms == 0 {
                return Err("schedule interval must be positive".to_string());
            }
            Ok(Some(after_ms.saturating_add(*every_ms)))
        }
        ScheduleTrigger::Cron {
            expression,
            timezone,
        } => {
            let schedule = Schedule::from_str(expression)
                .map_err(|error| format!("invalid cron expression: {error}"))?;
            let timezone = timezone
                .parse::<Tz>()
                .map_err(|_| format!("invalid IANA timezone: {timezone}"))?;
            let after = DateTime::<Utc>::from_timestamp_millis(after_ms as i64)
                .ok_or_else(|| "schedule timestamp is outside chrono range".to_string())?
                .with_timezone(&timezone);
            schedule
                .after(&after)
                .next()
                .map(|next| next.with_timezone(&Utc).timestamp_millis() as u64)
                .ok_or_else(|| "cron expression has no next occurrence".to_string())
                .map(Some)
        }
    }
}

fn load_state(
    event_store: &RuntimeEventStore,
    stream_id: &str,
) -> Result<MissionScheduleState, String> {
    event_store
        .latest_for_stream(stream_id)?
        .map(|event| {
            serde_json::from_value::<MissionScheduleStateEvent>(event.payload)
                .map(|event| active_only_state(&event.state))
                .map_err(|error| error.to_string())
        })
        .transpose()
        .map(|state| state.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::policy::{
        AutonomyProfileId, SessionExecutionPolicy, SessionExecutionPolicyOrigin,
    };

    fn create_request(trigger: ScheduleTrigger) -> CreateMissionScheduleRequest {
        CreateMissionScheduleRequest {
            mission_id: "mission-a".to_string(),
            target_session_id: "session-a".to_string(),
            objective: "inspect pending evidence".to_string(),
            trigger,
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            priority: 64,
        }
    }

    fn target_binding(ceiling: PermissionMode) -> ExecutionPolicyBinding {
        let policy = SessionExecutionPolicy::from_profile(
            AutonomyProfileId::Supervised,
            7,
            SessionExecutionPolicyOrigin::SessionExplicit,
        );
        ExecutionPolicyBinding::bind("session-a", &policy, ceiling)
    }

    #[test]
    fn due_interval_is_claimed_once_and_rebuilds_from_events() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
        let schedules =
            MissionScheduleStore::event_sourced(Arc::clone(&event_store), "workspace-a")
                .expect("schedule store");
        let schedule = schedules
            .create(
                create_request(ScheduleTrigger::Interval { every_ms: 100 }),
                1_000,
            )
            .expect("schedule");
        let first = schedules.claim_due(1_100, 10_000).expect("claim");
        assert_eq!(first.claimed.len(), 1);
        assert_eq!(first.claimed[0].schedule_id, schedule.schedule_id);
        assert!(schedules
            .claim_due(1_100, 10_000)
            .expect("dedupe")
            .claimed
            .is_empty());

        let rebuilt =
            MissionScheduleStore::event_sourced(event_store, "workspace-a").expect("rebuilt store");
        assert_eq!(
            rebuilt.projection()["schedules"].as_array().unwrap().len(),
            1
        );
        assert_eq!(rebuilt.pending_fires().len(), 1);
    }

    #[test]
    fn idle_ticks_do_not_append_duplicate_schedule_snapshots() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
        let schedules =
            MissionScheduleStore::event_sourced(Arc::clone(&event_store), "workspace-a")
                .expect("schedule store");
        let stream_id = "mission-schedules:workspace-a";

        let before = event_store
            .stream_revision(stream_id)
            .expect("initial revision");
        let first = schedules.claim_due(1_000, 10_000).expect("idle tick");
        let second = schedules.claim_due(2_000, 10_000).expect("idle tick");

        assert!(first.claimed.is_empty());
        assert!(second.claimed.is_empty());
        assert_eq!(
            event_store
                .stream_revision(stream_id)
                .expect("revision after idle ticks"),
            before,
            "idle timer observations must not create schedule snapshot events"
        );
    }

    #[test]
    fn manual_fire_keeps_automatic_cursor_and_delete_retains_fire_evidence() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
        let schedules = MissionScheduleStore::event_sourced(event_store, "workspace-a")
            .expect("schedule store");
        let schedule = schedules
            .create(
                create_request(ScheduleTrigger::Interval { every_ms: 60_000 }),
                1_000,
            )
            .expect("schedule");
        let next_at_ms = schedule.next_at_ms;

        let fire = schedules
            .trigger_now(&schedule.schedule_id, 2_000)
            .expect("manual fire");
        assert!(fire.fire_id.contains(":manual:"));
        assert_eq!(schedules.pending_fires(), vec![fire.clone()]);
        assert_eq!(
            schedules.projection()["schedules"][0]["next_at_ms"],
            next_at_ms
        );

        schedules
            .delete(&schedule.schedule_id)
            .expect("delete schedule");
        let projection = schedules.projection();
        assert!(projection["schedules"].as_array().unwrap().is_empty());
        assert_eq!(projection["fires"].as_array().unwrap().len(), 1);
        assert_eq!(projection["fires"][0]["fire_id"], fire.fire_id);
    }

    #[test]
    fn cron_requires_valid_expression_and_timezone() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
        let schedules = MissionScheduleStore::event_sourced(event_store, "workspace-a")
            .expect("schedule store");
        assert!(schedules
            .create(
                create_request(ScheduleTrigger::Cron {
                    expression: "not cron".to_string(),
                    timezone: "Mars/Olympus".to_string(),
                }),
                1_000,
            )
            .is_err());
    }

    #[test]
    fn update_requires_current_revision_and_recomputes_the_next_fire() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
        let schedules = MissionScheduleStore::event_sourced(event_store, "workspace-a")
            .expect("schedule store");
        let schedule = schedules
            .create(
                create_request(ScheduleTrigger::Interval { every_ms: 100 }),
                1_000,
            )
            .expect("schedule");

        assert!(schedules
            .update(
                &schedule.schedule_id,
                UpdateMissionScheduleRequest {
                    expected_revision: 0,
                    objective: Some("stale".to_string()),
                    trigger: None,
                    priority: None,
                },
                1_500,
            )
            .is_err());
        let updated = schedules
            .update(
                &schedule.schedule_id,
                UpdateMissionScheduleRequest {
                    expected_revision: schedule.revision,
                    objective: Some("inspect revised evidence".to_string()),
                    trigger: Some(ScheduleTrigger::Interval { every_ms: 500 }),
                    priority: Some(90),
                },
                1_500,
            )
            .expect("revision-checked update");
        assert_eq!(updated.revision, schedule.revision + 1);
        assert_eq!(updated.next_at_ms, 2_000);
        assert_eq!(updated.priority, 90);
        assert_eq!(updated.permission_ceiling, PermissionMode::ReadOnly);
    }

    #[test]
    fn fire_policy_is_bound_once_and_terminal_history_leaves_hot_state() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
        let schedules =
            MissionScheduleStore::event_sourced(Arc::clone(&event_store), "workspace-a")
                .expect("schedule store");
        let schedule = schedules
            .create(
                create_request(ScheduleTrigger::Interval { every_ms: 100 }),
                1_000,
            )
            .expect("schedule");
        let fire = schedules
            .trigger_now(&schedule.schedule_id, 1_100)
            .expect("fire");
        assert!(schedules
            .mark_submitted(&fire.fire_id, "graph-unbound".to_string())
            .is_err());
        let binding = target_binding(PermissionMode::ReadOnly);
        let bound = schedules
            .bind_target_policy(&fire.fire_id, binding.clone())
            .expect("bind policy");
        assert_eq!(bound.target_policy_binding.as_ref(), Some(&binding));
        assert_eq!(
            schedules
                .bind_target_policy(&fire.fire_id, binding)
                .expect("idempotent bind"),
            bound
        );
        drop(schedules);
        let schedules =
            MissionScheduleStore::event_sourced(Arc::clone(&event_store), "workspace-a")
                .expect("rebuild pending fire");
        assert_eq!(
            schedules.pending_fires()[0].target_policy_binding,
            bound.target_policy_binding
        );

        let submitted = schedules
            .mark_submitted(&fire.fire_id, "graph-a".to_string())
            .expect("submitted");
        assert_eq!(submitted.status, MissionScheduleFireStatus::Submitted);
        assert_eq!(schedules.active_fire_count(), 1);
        assert_eq!(schedules.submitted_fires(), vec![submitted.clone()]);
        assert_eq!(
            schedules.fire_by_id(&fire.fire_id).expect("lookup"),
            Some(submitted.clone())
        );
        assert_eq!(
            schedules
                .mark_submitted(&fire.fire_id, "graph-a".to_string())
                .expect("idempotent terminal replay"),
            submitted
        );
        let completed = schedules.mark_completed(&fire.fire_id).expect("completed");
        assert_eq!(completed.status, MissionScheduleFireStatus::Completed);
        assert_eq!(schedules.active_fire_count(), 0);

        let history = schedules.fire_history_page(None, 2).expect("history page");
        assert_eq!(history.fires.len(), 2);
        assert!(history.has_more);
        let second = schedules
            .fire_history_page(history.next_cursor, 10)
            .expect("second history page");
        assert!(!second.fires.is_empty());
        assert_eq!(
            second.fires.last().unwrap().status,
            MissionScheduleFireStatus::Completed
        );

        let rebuilt =
            MissionScheduleStore::event_sourced(event_store, "workspace-a").expect("rebuilt store");
        assert_eq!(rebuilt.active_fire_count(), 0);
        assert_eq!(
            rebuilt.fire_by_id(&fire.fire_id).expect("rebuilt lookup"),
            Some(completed)
        );
    }

    #[test]
    fn ten_thousand_terminal_fires_do_not_accumulate_in_hot_snapshots() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
        let schedules =
            MissionScheduleStore::event_sourced(Arc::clone(&event_store), "workspace-a")
                .expect("schedule store");
        let mut terminals = Vec::with_capacity(10_000);
        let binding = target_binding(PermissionMode::ReadOnly);
        let mut state = schedules
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for index in 0..10_000_u64 {
            let fire_id = format!("fire-{index}");
            let fire = MissionScheduleFire {
                fire_id: fire_id.clone(),
                schedule_id: "schedule-a".to_string(),
                mission_id: "mission-a".to_string(),
                due_at_ms: index,
                correlation_id: format!("schedule-fire:fire-{index}"),
                target_session_id: "session-a".to_string(),
                objective: "bounded history".to_string(),
                permission_ceiling: PermissionMode::ReadOnly,
                target_policy_binding: Some(binding.clone()),
                priority: 1,
                status: MissionScheduleFireStatus::Submitted,
                graph_id: Some(format!("graph-{index}")),
                error: None,
            };
            state.fires.insert(fire.fire_id.clone(), fire);
            terminals.push(MissionScheduleFireTerminal::Completed { fire_id });
        }
        drop(state);

        assert_eq!(
            schedules
                .mark_terminal_batch(terminals)
                .expect("chunked terminal wave")
                .len(),
            10_000
        );
        assert_eq!(schedules.active_fire_count(), 0);
        assert_eq!(
            event_store
                .stream_event_count("mission-schedules:workspace-a")
                .expect("event count"),
            10_002,
            "two bounded transactions each append terminal receipts then one hot snapshot"
        );
        let rebuilt = MissionScheduleStore::event_sourced(event_store, "workspace-a")
            .expect("rebuild after terminal wave");
        assert_eq!(rebuilt.active_fire_count(), 0);
    }
}
