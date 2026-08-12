//! Runtime-owned Mission aggregates.
//!
//! Canonical Session lifecycle, working-set state, presence, branching and
//! input admission are owned by Gateway SessionService. Mission owns its goal
//! and lifecycle only; Task assignment is the sole membership authority.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use harness_contract::mission::{MissionAggregate, MissionMutationReceipt, MissionStatus};
use harness_contract::reality::EvidenceRef;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    AgentRuntime, ApprovalQueue, ConflictArbiter, MissionEvidenceBus, RuntimeCapabilityCatalog,
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore, SessionRelationGraph,
    TeamRuntime,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionProjection {
    pub kind: String,
    pub schema_version: u32,
    pub mission_id: Option<String>,
    pub aggregate: Option<MissionAggregate>,
    pub team_projection: serde_json::Value,
    pub agent_projection: serde_json::Value,
    pub approval_projection: serde_json::Value,
    pub relation_projection: serde_json::Value,
    pub execution_graph_projection: serde_json::Value,
    pub conflict_projection: serde_json::Value,
    pub evidence_projection: serde_json::Value,
    pub schedule_projection: serde_json::Value,
    pub capability_projection: serde_json::Value,
    pub health_projection: serde_json::Value,
    pub recovery_projection: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MissionAggregateEvent {
    event_kind: String,
    aggregate: MissionAggregate,
    evidence_refs: Vec<EvidenceRef>,
}

#[derive(Debug)]
pub struct MissionRuntime {
    missions: Mutex<BTreeMap<String, MissionAggregate>>,
    event_store: Option<Arc<RuntimeEventStore>>,
    workspace_id: String,
    default_mission_id: String,
}

impl Default for MissionRuntime {
    fn default() -> Self {
        let workspace_id = "in-memory".to_string();
        let default_mission_id = deterministic_default_mission_id(&workspace_id);
        Self {
            missions: Mutex::new(BTreeMap::new()),
            event_store: None,
            workspace_id,
            default_mission_id,
        }
    }
}

impl MissionRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn event_sourced(
        event_store: Arc<RuntimeEventStore>,
        workspace_id: impl Into<String>,
    ) -> Result<Self, String> {
        let workspace_id = workspace_id.into();
        let default_mission_id = deterministic_default_mission_id(&workspace_id);
        let missions = load_missions(&event_store, &workspace_id)?;
        Ok(Self {
            missions: Mutex::new(missions),
            event_store: Some(event_store),
            workspace_id,
            default_mission_id,
        })
    }

    #[must_use]
    pub fn default_mission_id(&self) -> &str {
        &self.default_mission_id
    }

    pub fn create_mission(
        &self,
        mission_id: impl Into<String>,
        objective: impl Into<String>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionAggregate, String> {
        let mission_id = mission_id.into();
        let objective = objective.into();
        validate_required("mission_id", &mission_id)?;
        validate_required("objective", &objective)?;
        let mut missions = self
            .missions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = missions.get(&mission_id) {
            if existing.objective == objective {
                return Ok(existing.clone());
            }
            return Err(format!(
                "mission `{mission_id}` already exists with another objective"
            ));
        }
        let aggregate = initial_aggregate(mission_id.clone(), self.workspace_id.clone(), objective);
        if let Some(event_store) = &self.event_store {
            append_aggregate_event(
                event_store,
                &aggregate,
                0,
                "mission.created",
                &evidence_refs,
            )?;
        }
        missions.insert(mission_id, aggregate.clone());
        Ok(aggregate)
    }

    pub fn ensure_default_mission(&self) -> Result<MissionAggregate, String> {
        if let Some(existing) = self.aggregate(self.default_mission_id()) {
            return Ok(existing);
        }
        self.create_mission(
            self.default_mission_id().to_string(),
            format!("Workspace mission for {}", self.workspace_id),
            Vec::new(),
        )
    }

    #[must_use]
    pub fn aggregate(&self, mission_id: &str) -> Option<MissionAggregate> {
        self.missions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(mission_id)
            .cloned()
    }

    #[must_use]
    pub fn aggregates(&self) -> Vec<MissionAggregate> {
        self.missions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn activate_if_draft(
        &self,
        mission_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.mutate_aggregate_current(
            mission_id,
            "mission.active".to_string(),
            evidence_refs,
            |aggregate| {
                if aggregate.status == MissionStatus::Draft {
                    aggregate.status = MissionStatus::Active;
                }
                Ok(())
            },
        )
    }

    pub fn update_strategy_ref(
        &self,
        mission_id: &str,
        expected_revision: u64,
        strategy_ref: Option<String>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        if strategy_ref
            .as_deref()
            .is_some_and(|strategy_ref| strategy_ref.trim().is_empty())
        {
            return Err("strategy_ref must not be blank".to_string());
        }
        self.mutate_aggregate(
            mission_id,
            expected_revision,
            "mission.strategy.updated".to_string(),
            evidence_refs,
            move |aggregate| {
                aggregate.strategy_ref = strategy_ref;
                Ok(())
            },
        )
    }

    pub fn transition(
        &self,
        mission_id: &str,
        expected_revision: u64,
        status: MissionStatus,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.mutate_aggregate(
            mission_id,
            expected_revision,
            format!("mission.{}", status.as_str()),
            evidence_refs,
            move |aggregate| {
                validate_mission_transition(aggregate.status, status)?;
                aggregate.status = status;
                Ok(())
            },
        )
    }

    #[must_use]
    pub fn revision(&self) -> Result<u64, String> {
        self.aggregate(self.default_mission_id())
            .map(|aggregate| aggregate.revision)
            .ok_or_else(|| "default mission aggregate is missing".to_string())
    }

    pub fn projection(
        &self,
        relations: &SessionRelationGraph,
        agent_runtime: &AgentRuntime,
        team_runtime: &TeamRuntime,
        approval_queue: &ApprovalQueue,
        conflict_resolver: &ConflictArbiter,
        mission_evidence: &MissionEvidenceBus,
        schedule_projection: serde_json::Value,
    ) -> MissionProjection {
        self.projection_for(
            self.default_mission_id(),
            relations,
            agent_runtime,
            team_runtime,
            approval_queue,
            conflict_resolver,
            mission_evidence,
            schedule_projection,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn projection_for(
        &self,
        mission_id: &str,
        relations: &SessionRelationGraph,
        agent_runtime: &AgentRuntime,
        team_runtime: &TeamRuntime,
        approval_queue: &ApprovalQueue,
        conflict_resolver: &ConflictArbiter,
        mission_evidence: &MissionEvidenceBus,
        schedule_projection: serde_json::Value,
    ) -> MissionProjection {
        let aggregate = self.aggregate(mission_id);
        // Entity membership is derived from typed Task lineage by Mission
        // Control. This shallow runtime projection only selects entities that
        // already carry an explicit mission identity; it never maintains a
        // second writable member list on MissionAggregate.
        let team_projection =
            filter_projection_array(team_runtime.projection_json(), "teams", |team| {
                value_has_mission(team, mission_id)
            });
        let agent_projection = filter_projection_array(
            serde_json::json!({
                "kind": "runtime.agents",
                "agents": agent_runtime.list(),
            }),
            "agents",
            |agent| value_has_mission(agent, mission_id),
        );
        let approval_projection =
            filter_projection_array(approval_queue.projection(), "requests", |approval| {
                value_has_mission(approval, mission_id)
            });
        let approval_projection = filter_projection_array(approval_projection, "grants", |grant| {
            value_has_mission(grant, mission_id)
        });
        let relation_projection =
            filter_projection_array(relations.projection(), "relations", |relation| {
                value_has_mission(relation, mission_id)
            });
        let relation_projection =
            filter_projection_array(relation_projection, "proxies", |proxy| {
                value_has_mission(proxy, mission_id)
            });
        let execution_graph_projection = serde_json::json!({
            "kind": "runtime.mission_execution_graphs",
            "count": 0,
            "execution_graphs": [],
            "relation_source": "task_lineage",
            "shallow": true,
        });
        let conflict_projection =
            filter_projection_array(conflict_resolver.projection(), "receipts", |receipt| {
                value_has_mission(receipt, mission_id)
            });
        let evidence_projection =
            filter_projection_array(mission_evidence.projection(), "latest", |evidence| {
                value_has_mission(evidence, mission_id)
            });
        let schedule_projection =
            filter_mission_schedule_projection(schedule_projection, mission_id);
        MissionProjection {
            kind: "mission.runtime".to_string(),
            schema_version: 6,
            mission_id: aggregate
                .as_ref()
                .map(|aggregate| aggregate.mission_id.clone()),
            aggregate,
            team_projection,
            agent_projection,
            approval_projection,
            relation_projection,
            execution_graph_projection,
            conflict_projection,
            evidence_projection,
            schedule_projection,
            capability_projection: serde_json::json!(RuntimeCapabilityCatalog::current()),
            health_projection: mission_health_projection(),
            recovery_projection: mission_recovery_projection(),
        }
    }

    fn mutate_aggregate(
        &self,
        mission_id: &str,
        expected_revision: u64,
        event_kind: String,
        evidence_refs: Vec<EvidenceRef>,
        mutation: impl FnOnce(&mut MissionAggregate) -> Result<(), String>,
    ) -> Result<MissionMutationReceipt, String> {
        self.mutate_aggregate_guarded(
            mission_id,
            Some(expected_revision),
            event_kind,
            evidence_refs,
            mutation,
        )
    }

    fn mutate_aggregate_current(
        &self,
        mission_id: &str,
        event_kind: String,
        evidence_refs: Vec<EvidenceRef>,
        mutation: impl FnOnce(&mut MissionAggregate) -> Result<(), String>,
    ) -> Result<MissionMutationReceipt, String> {
        self.mutate_aggregate_guarded(mission_id, None, event_kind, evidence_refs, mutation)
    }

    fn mutate_aggregate_guarded(
        &self,
        mission_id: &str,
        expected_revision: Option<u64>,
        event_kind: String,
        evidence_refs: Vec<EvidenceRef>,
        mutation: impl FnOnce(&mut MissionAggregate) -> Result<(), String>,
    ) -> Result<MissionMutationReceipt, String> {
        let mut missions = self
            .missions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = missions
            .get(mission_id)
            .cloned()
            .ok_or_else(|| format!("mission `{mission_id}` not found"))?;
        if let Some(expected_revision) = expected_revision {
            if current.revision != expected_revision {
                return Err(format!(
                    "stale mission revision: expected {expected_revision}, actual {}",
                    current.revision
                ));
            }
        }
        let mut next = current.clone();
        mutation(&mut next)?;
        if next == current {
            return Ok(MissionMutationReceipt {
                mission_id: current.mission_id,
                accepted_revision: current.revision,
                status: current.status,
                evidence_refs,
            });
        }
        next.revision = current.revision.saturating_add(1);
        next.updated_at_ms = now_ms();
        if next.status.is_terminal() && evidence_refs.is_empty() {
            return Err("terminal Mission transition requires evidence".to_string());
        }
        if let Some(event_store) = &self.event_store {
            if let Err(error) = append_aggregate_event(
                event_store,
                &next,
                current.revision,
                &event_kind,
                &evidence_refs,
            ) {
                if let Some(restored) = load_mission(event_store, mission_id)? {
                    missions.insert(mission_id.to_string(), restored);
                }
                return Err(error);
            }
        }
        missions.insert(mission_id.to_string(), next.clone());
        Ok(MissionMutationReceipt {
            mission_id: next.mission_id,
            accepted_revision: next.revision,
            status: next.status,
            evidence_refs,
        })
    }
}

fn append_aggregate_event(
    event_store: &RuntimeEventStore,
    aggregate: &MissionAggregate,
    expected_revision: u64,
    event_kind: &str,
    evidence_refs: &[EvidenceRef],
) -> Result<(), String> {
    let mut refs = vec![
        RuntimeEventRef {
            kind: "mission".to_string(),
            id: aggregate.mission_id.clone(),
        },
        RuntimeEventRef {
            kind: "workspace".to_string(),
            id: aggregate.workspace_id.clone(),
        },
    ];
    refs.extend(evidence_refs.iter().map(|reference| RuntimeEventRef {
        kind: reference.ref_type.clone(),
        id: reference.id.clone(),
    }));
    refs.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.id.cmp(&right.id))
    });
    refs.dedup_by(|left, right| left.kind == right.kind && left.id == right.id);
    event_store
        .append_batch_if_revision(
            mission_stream_id(&aggregate.mission_id),
            expected_revision,
            format!(
                "mission:{}:revision:{}",
                aggregate.mission_id, aggregate.revision
            ),
            vec![RuntimeEventInput {
                stream_id: mission_stream_id(&aggregate.mission_id),
                scope: RuntimeEventScope::Mission,
                kind: format!("{event_kind}.v1"),
                status: Some(aggregate.status.as_str().to_string()),
                actor: Some("mission_runtime".to_string()),
                refs,
                payload: serde_json::to_value(MissionAggregateEvent {
                    event_kind: event_kind.to_string(),
                    aggregate: aggregate.clone(),
                    evidence_refs: evidence_refs.to_vec(),
                })
                .map_err(|error| error.to_string())?,
            }
            .into()],
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn load_missions(
    event_store: &RuntimeEventStore,
    workspace_id: &str,
) -> Result<BTreeMap<String, MissionAggregate>, String> {
    let mut missions = BTreeMap::new();
    for stream_id in event_store
        .stream_ids_for_scope(RuntimeEventScope::Mission)
        .map_err(|error| error.to_string())?
    {
        let Some(mission_id) = stream_id.strip_prefix("mission:") else {
            continue;
        };
        if let Some(aggregate) = load_mission(event_store, mission_id)? {
            if aggregate.workspace_id == workspace_id {
                missions.insert(mission_id.to_string(), aggregate);
            }
        }
    }
    Ok(missions)
}

fn load_mission(
    event_store: &RuntimeEventStore,
    mission_id: &str,
) -> Result<Option<MissionAggregate>, String> {
    event_store
        .list_stream(&mission_stream_id(mission_id))?
        .into_iter()
        .rev()
        .find_map(|event| {
            event
                .kind
                .starts_with("mission.")
                .then(|| serde_json::from_value::<MissionAggregateEvent>(event.payload))
        })
        .transpose()
        .map_err(|error| error.to_string())
        .map(|event| event.map(|event| event.aggregate))
}

fn initial_aggregate(
    mission_id: String,
    workspace_id: String,
    objective: String,
) -> MissionAggregate {
    let now = now_ms();
    MissionAggregate {
        mission_id,
        workspace_id,
        objective,
        status: MissionStatus::Draft,
        revision: 1,
        strategy_ref: None,
        created_at_ms: now,
        updated_at_ms: now,
    }
}

fn deterministic_default_mission_id(workspace_id: &str) -> String {
    let digest = format!("{:x}", Sha256::digest(workspace_id.as_bytes()));
    format!("mission-default-{}", &digest[..16])
}

fn mission_stream_id(mission_id: &str) -> String {
    format!("mission:{mission_id}")
}

fn validate_mission_transition(from: MissionStatus, to: MissionStatus) -> Result<(), String> {
    let allowed = matches!(
        (from, to),
        (MissionStatus::Draft, MissionStatus::Active)
            | (MissionStatus::Active, MissionStatus::Paused)
            | (MissionStatus::Paused, MissionStatus::Active)
            | (
                MissionStatus::Active | MissionStatus::Paused,
                MissionStatus::Completed | MissionStatus::Cancelled | MissionStatus::Failed
            )
    );
    if !allowed {
        return Err(format!(
            "illegal Mission transition {} -> {}",
            from.as_str(),
            to.as_str()
        ));
    }
    Ok(())
}

fn validate_required(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("Mission `{field}` is required"));
    }
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn filter_projection_array(
    mut projection: serde_json::Value,
    field: &str,
    include: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let Some((len, pending_count, active_grant_count)) = projection
        .get_mut(field)
        .and_then(serde_json::Value::as_array_mut)
        .map(|values| {
            values.retain(include);
            let pending_count = (field == "requests").then(|| {
                values
                    .iter()
                    .filter(|request| request["status"].as_str() == Some("pending"))
                    .count()
            });
            let active_grant_count = (field == "grants").then(|| {
                let now = now_ms();
                values
                    .iter()
                    .filter(|grant| {
                        grant["status"].as_str() == Some("active")
                            && grant["expires_at_ms"]
                                .as_u64()
                                .is_none_or(|expires| expires > now)
                    })
                    .count()
            });
            (values.len(), pending_count, active_grant_count)
        })
    else {
        return projection;
    };
    match field {
        "requests" => {
            if let Some(count) = projection.get_mut("count") {
                *count = serde_json::json!(len);
            }
            if let Some(pending_count) = pending_count {
                if let Some(count) = projection.get_mut("pending_count") {
                    *count = serde_json::json!(pending_count);
                }
            }
        }
        "grants" => {
            if let Some(count) = projection.get_mut("active_grant_count") {
                *count = serde_json::json!(active_grant_count.unwrap_or_default());
            }
        }
        "relations" => {
            if let Some(count) = projection.get_mut("relation_count") {
                *count = serde_json::json!(len);
            }
        }
        "proxies" => {
            if let Some(count) = projection.get_mut("proxy_count") {
                *count = serde_json::json!(len);
            }
        }
        _ => {
            if let Some(count) = projection.get_mut("count") {
                *count = serde_json::json!(len);
            }
        }
    }
    projection
}

fn value_has_mission(value: &serde_json::Value, mission_id: &str) -> bool {
    [
        "/mission_id",
        "/execution_identity/mission_id",
        "/source/mission_id",
        "/scope/mission_id",
    ]
    .iter()
    .any(|pointer| value.pointer(pointer).and_then(serde_json::Value::as_str) == Some(mission_id))
}

fn filter_mission_schedule_projection(
    mut projection: serde_json::Value,
    mission_id: &str,
) -> serde_json::Value {
    let schedule_ids = projection["schedules"]
        .as_array_mut()
        .map(|schedules| {
            schedules.retain(|schedule| schedule["mission_id"].as_str() == Some(mission_id));
            schedules
                .iter()
                .filter_map(|schedule| schedule["schedule_id"].as_str().map(str::to_owned))
                .collect::<std::collections::BTreeSet<_>>()
        })
        .unwrap_or_default();
    if let Some(fires) = projection["fires"].as_array_mut() {
        fires.retain(|fire| {
            fire["mission_id"].as_str() == Some(mission_id)
                || fire["schedule_id"]
                    .as_str()
                    .is_some_and(|id| schedule_ids.contains(id))
        });
    }
    projection
}

fn mission_health_projection() -> serde_json::Value {
    serde_json::json!({
        "kind": "runtime.mission_health",
        "ok": true,
        "status": "ready",
        "degraded_reasons": [],
        "session_owner": "gateway.session_service",
        "aggregate_owner": "runtime.mission",
    })
}

fn mission_recovery_projection() -> serde_json::Value {
    serde_json::json!({
        "kind": "runtime.mission_recovery",
        "candidate_count": 0,
        "candidates": [],
        "owner": "runtime_mission_aggregate",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mission_revision_cas_and_terminal_evidence_are_enforced() {
        let runtime = MissionRuntime::new();
        let mission = runtime.ensure_default_mission().expect("mission");
        assert!(runtime
            .transition(
                runtime.default_mission_id(),
                mission.revision + 1,
                MissionStatus::Active,
                Vec::new()
            )
            .is_err());
        let activated = runtime
            .transition(
                runtime.default_mission_id(),
                mission.revision,
                MissionStatus::Active,
                Vec::new(),
            )
            .expect("activate");
        assert!(runtime
            .transition(
                runtime.default_mission_id(),
                activated.accepted_revision,
                MissionStatus::Completed,
                Vec::new()
            )
            .is_err());
    }
}
