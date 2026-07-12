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
    MissionSchedule, MissionScheduleFire, MissionScheduleStatus, ScheduleTrigger,
};
use serde::{Deserialize, Serialize};

use crate::runtime_event_store::RuntimeTransactionEventInput;
use crate::{RuntimeEventInput, RuntimeEventScope, RuntimeEventStore};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateMissionScheduleRequest {
    pub mission_id: String,
    pub target_session_id: String,
    pub objective: String,
    pub trigger: ScheduleTrigger,
    pub autonomy_profile: String,
    pub permission_lease: String,
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
    pub autonomy_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_lease: Option<String>,
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct MissionScheduleState {
    schedules: BTreeMap<String, MissionSchedule>,
    fires: BTreeMap<String, MissionScheduleFire>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MissionScheduleStateEvent {
    state: MissionScheduleState,
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
            autonomy_profile: request.autonomy_profile,
            permission_lease: request.permission_lease,
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
            if let Some(autonomy_profile) = request.autonomy_profile {
                if autonomy_profile.trim().is_empty() {
                    return Err("schedule autonomy_profile must not be empty".to_string());
                }
                schedule.autonomy_profile = autonomy_profile;
            }
            if let Some(permission_lease) = request.permission_lease {
                if permission_lease.trim().is_empty() {
                    return Err("schedule permission_lease must not be empty".to_string());
                }
                schedule.permission_lease = permission_lease;
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
                    "missed"
                } else {
                    "pending"
                };
                let fire = MissionScheduleFire {
                    correlation_id: format!("schedule-fire:{fire_id}"),
                    fire_id: fire_id.clone(),
                    schedule_id: schedule.schedule_id.clone(),
                    due_at_ms,
                    target_session_id: schedule.target_session_id.clone(),
                    objective: schedule.objective.clone(),
                    permission_lease: schedule.permission_lease.clone(),
                    priority: schedule.priority,
                    status: status.to_string(),
                    graph_id: None,
                    error: (status == "missed").then(|| {
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
                if status == "missed" {
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

    pub fn mark_submitted(
        &self,
        fire_id: &str,
        graph_id: String,
    ) -> Result<MissionScheduleFire, String> {
        self.mutate("mission.schedule.submitted.v1", |state| {
            let fire = state
                .fires
                .get_mut(fire_id)
                .ok_or_else(|| format!("schedule fire not found: {fire_id}"))?;
            if fire.status != "pending" && fire.status != "submitted" {
                return Err(format!(
                    "schedule fire {fire_id} cannot submit from {}",
                    fire.status
                ));
            }
            fire.status = "submitted".to_string();
            fire.graph_id = Some(graph_id);
            Ok(fire.clone())
        })
    }

    pub fn mark_failed(&self, fire_id: &str, error: String) -> Result<MissionScheduleFire, String> {
        self.mutate("mission.schedule.failed.v1", |state| {
            let fire = state
                .fires
                .get_mut(fire_id)
                .ok_or_else(|| format!("schedule fire not found: {fire_id}"))?;
            fire.status = "failed".to_string();
            fire.error = Some(error);
            Ok(fire.clone())
        })
    }

    #[must_use]
    pub fn pending_fires(&self) -> Vec<MissionScheduleFire> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fires
            .values()
            .filter(|fire| fire.status == "pending")
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
        // A timer tick that finds no due schedule is an observation, not a
        // domain mutation. Persisting a complete snapshot for every empty
        // tick bloats the event stream and can contend with foreground turn
        // execution. Only durable state transitions receive an event.
        if *state == previous {
            return Ok(result);
        }
        if let Err(error) = self.commit(&state, event_kind) {
            *state = previous;
            return Err(error);
        }
        Ok(result)
    }

    fn commit(&self, state: &MissionScheduleState, event_kind: &str) -> Result<(), String> {
        let revision = self
            .event_store
            .stream_revision(&self.stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                self.stream_id.clone(),
                revision,
                format!(
                    "mission-schedules:{}:{}",
                    self.stream_id,
                    revision.saturating_add(1)
                ),
                vec![RuntimeTransactionEventInput {
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
                        "schedule-revision:{}",
                        revision.saturating_add(1)
                    )),
                    schema_version: 1,
                }],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

fn validate_create_request(request: &CreateMissionScheduleRequest) -> Result<(), String> {
    for (name, value) in [
        ("mission_id", request.mission_id.as_str()),
        ("target_session_id", request.target_session_id.as_str()),
        ("objective", request.objective.as_str()),
        ("permission_lease", request.permission_lease.as_str()),
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
                .map(|event| event.state)
                .map_err(|error| error.to_string())
        })
        .transpose()
        .map(|state| state.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_request(trigger: ScheduleTrigger) -> CreateMissionScheduleRequest {
        CreateMissionScheduleRequest {
            mission_id: "mission-a".to_string(),
            target_session_id: "session-a".to_string(),
            objective: "inspect pending evidence".to_string(),
            trigger,
            autonomy_profile: "assisted".to_string(),
            permission_lease: "read_only".to_string(),
            priority: 64,
        }
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
                    autonomy_profile: None,
                    permission_lease: None,
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
                    autonomy_profile: Some("supervised".to_string()),
                    permission_lease: Some("reviewed".to_string()),
                    priority: Some(90),
                },
                1_500,
            )
            .expect("revision-checked update");
        assert_eq!(updated.revision, schedule.revision + 1);
        assert_eq!(updated.next_at_ms, 2_000);
        assert_eq!(updated.priority, 90);
    }
}
