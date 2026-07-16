//! Runtime-owned Managed Agent definitions, invocations and fenced effects.
//!
//! Gateway and connectors only submit declarative definitions or normalized
//! events.  This dispatcher owns deduplication, overlap decisions, durable
//! fencing, retry state, and effect receipts.  It never keeps a long-lived
//! model process: every accepted invocation is later compiled by
//! `RuntimeServices` into a fresh Agent Binding or Team graph.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use harness_contract::managed_agent::{
    ManagedAgentDefinition, ManagedAgentEventOrderPolicy, ManagedAgentEventTrigger,
    ManagedAgentOverlapPolicy, ManagedAgentTrigger, ManagedAgentTriggerEvent,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runtime_event_store::{RuntimeEventStoreError, RuntimeTransactionEventInput};
use crate::{RuntimeEventInput, RuntimeEventScope, RuntimeEventStore};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedAgentInvocationStatus {
    Pending,
    Claimed,
    Running,
    RetryScheduled,
    Completed,
    Failed,
    SuppressedOverlap,
    Cancelled,
    ReconciliationRequired,
}

impl ManagedAgentInvocationStatus {
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Pending | Self::Claimed | Self::Running | Self::RetryScheduled
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ManagedAgentInvocationTrigger {
    Manual {
        request_id: String,
    },
    Schedule {
        due_at_ms: u64,
    },
    Event {
        event_id: String,
        source_id: String,
        source_kind: String,
        event_type: String,
        payload_ref: String,
        payload_digest: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedAgentInvocation {
    pub invocation_id: String,
    pub definition_id: String,
    pub definition_revision: u64,
    pub trigger: ManagedAgentInvocationTrigger,
    pub status: ManagedAgentInvocationStatus,
    pub attempt_no: u16,
    pub fence_generation: u64,
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FencedEffectStatus {
    Pending,
    Claimed,
    Completed,
    ReconciliationRequired,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FencedEffectOutboxRecord {
    pub effect_id: String,
    pub invocation_id: String,
    pub attempt_no: u16,
    pub fence_generation: u64,
    pub effect_kind: String,
    pub idempotency_key: String,
    pub request_ref: String,
    pub status: FencedEffectStatus,
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedAgentDispatchReport {
    pub accepted: Vec<ManagedAgentInvocation>,
    pub suppressed: Vec<ManagedAgentInvocation>,
    pub rejected: Vec<String>,
}

/// Runtime execution result for one dispatcher tick.  The scheduler accepts
/// triggers durably first, then reports exactly which claimed invocations
/// reached a terminal state in this process.  A caller must consult the
/// invocation projection for retries and reconciliation-required work rather
/// than treating this ephemeral report as the source of truth.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedAgentRuntimeDispatchReport {
    pub health_affected: Vec<ManagedAgentInvocation>,
    pub scheduled: ManagedAgentDispatchReport,
    pub claimed: Vec<ManagedAgentInvocation>,
    pub completed: Vec<ManagedAgentInvocation>,
    pub failed: Vec<ManagedAgentInvocation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedAgentHealthStatus {
    Healthy,
    Degraded,
    CircuitOpen,
    ReconciliationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedAgentHealth {
    pub managed_agent_id: String,
    pub revision: u64,
    pub status: ManagedAgentHealthStatus,
    pub consecutive_failures: u16,
    pub max_consecutive_failures: u16,
    pub active_invocation_ids: Vec<String>,
}

/// Runtime's decision at an external-effect boundary.  A completed receipt
/// is deliberately not executed again: callers can surface the receipt to
/// the model while preserving at-most-once behaviour across retries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ManagedAgentEffectPermit {
    Execute { record: FencedEffectOutboxRecord },
    AlreadyCompleted { record: FencedEffectOutboxRecord },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ManagedAgentState {
    definitions: BTreeMap<String, BTreeMap<u64, ManagedAgentDefinition>>,
    active_revisions: BTreeMap<String, u64>,
    invocations: BTreeMap<String, ManagedAgentInvocation>,
    outbox: BTreeMap<String, FencedEffectOutboxRecord>,
    schedule_next_at_ms: BTreeMap<String, u64>,
    consecutive_failures: BTreeMap<String, u16>,
    /// Highest accepted source sequence per immutable definition/source/
    /// subject partition. It is only consulted by definitions that opt into
    /// `RejectOlderSequence`; default idempotent feeds keep accepting replay.
    event_sequences: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManagedAgentStateEvent {
    state: ManagedAgentState,
}

/// One workspace-scoped event-sourced dispatcher.
pub struct ManagedAgentDispatcher {
    event_store: Arc<RuntimeEventStore>,
    stream_id: String,
    /// The event store's CAS is the cross-process fence.  This lock simply
    /// avoids needless local retries while a single Runtime process emits
    /// closely spaced triggers.
    mutation_lock: Mutex<()>,
}

impl ManagedAgentDispatcher {
    /// Build an isolated dispatcher backed by Runtime's in-memory event
    /// store. This is intentionally a dispatcher-level fixture: callers can
    /// exercise the real durable state machine without receiving a raw
    /// control-plane event-store write handle.
    pub fn in_memory() -> Result<Self, String> {
        Self::event_sourced(
            Arc::new(
                RuntimeEventStore::try_open_in_memory().map_err(|error| {
                    format!("open managed Agent in-memory event store: {error}")
                })?,
            ),
            "managed-agent-in-memory",
        )
    }

    pub fn event_sourced(
        event_store: Arc<RuntimeEventStore>,
        workspace_key: impl Into<String>,
    ) -> Result<Self, String> {
        let stream_id = format!("managed-agents:{}", workspace_key.into());
        // Fail early for a corrupt projection rather than accepting new work
        // on top of an unknown durable state.
        let _ = load_state(&event_store, &stream_id)?;
        Ok(Self {
            event_store,
            stream_id,
            mutation_lock: Mutex::new(()),
        })
    }

    pub fn register_definition(
        &self,
        definition: ManagedAgentDefinition,
        now_ms: u64,
    ) -> Result<ManagedAgentDefinition, String> {
        definition.validate().map_err(|error| error.to_string())?;
        self.mutate("managed_agent.definition.registered.v1", |state| {
            let revisions = state
                .definitions
                .entry(definition.managed_agent_id.clone())
                .or_default();
            if let Some(existing) = revisions.get(&definition.revision) {
                if existing == &definition {
                    return Ok(existing.clone());
                }
                return Err(format!(
                    "managed Agent definition `{}` revision {} already exists with different content",
                    definition.managed_agent_id, definition.revision
                ));
            }
            if let Some(latest) = revisions.keys().next_back().copied() {
                if definition.revision != latest.saturating_add(1) {
                    return Err(format!(
                        "managed Agent definition `{}` revision must advance from {} to {}",
                        definition.managed_agent_id,
                        latest,
                        latest.saturating_add(1)
                    ));
                }
            }
            if revisions.is_empty() && definition.revision != 1 {
                return Err("first managed Agent definition revision must be 1".to_string());
            }
            revisions.insert(definition.revision, definition.clone());
            state
                .active_revisions
                .insert(definition.managed_agent_id.clone(), definition.revision);
            if let ManagedAgentTrigger::Schedule { trigger } = &definition.trigger {
                let key = definition_key(&definition.managed_agent_id, definition.revision);
                state.schedule_next_at_ms.insert(
                    key,
                    next_schedule_after(trigger, now_ms.saturating_sub(1))?.ok_or_else(|| {
                        "managed Agent schedule has no next occurrence".to_string()
                    })?,
                );
            }
            Ok(definition.clone())
        })
    }

    pub fn definitions(&self) -> Result<Vec<ManagedAgentDefinition>, String> {
        let state = self.state()?;
        let mut values = state
            .active_revisions
            .iter()
            .filter_map(|(id, revision)| {
                state
                    .definitions
                    .get(id)
                    .and_then(|versions| versions.get(revision))
                    .cloned()
            })
            .collect::<Vec<_>>();
        values.sort_by(|left, right| left.managed_agent_id.cmp(&right.managed_agent_id));
        Ok(values)
    }

    pub fn definition(
        &self,
        managed_agent_id: &str,
        revision: Option<u64>,
    ) -> Result<ManagedAgentDefinition, String> {
        let state = self.state()?;
        let revision = revision
            .or_else(|| state.active_revisions.get(managed_agent_id).copied())
            .ok_or_else(|| format!("managed Agent definition not found: {managed_agent_id}"))?;
        state
            .definitions
            .get(managed_agent_id)
            .and_then(|versions| versions.get(&revision))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "managed Agent definition revision not found: {managed_agent_id}@{revision}"
                )
            })
    }

    pub fn invocations(&self) -> Result<Vec<ManagedAgentInvocation>, String> {
        let state = self.state()?;
        let mut values = state.invocations.into_values().collect::<Vec<_>>();
        values.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        Ok(values)
    }

    pub fn outbox(&self) -> Result<Vec<FencedEffectOutboxRecord>, String> {
        let state = self.state()?;
        let mut values = state.outbox.into_values().collect::<Vec<_>>();
        values.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        Ok(values)
    }

    pub fn health(&self) -> Result<Vec<ManagedAgentHealth>, String> {
        let state = self.state()?;
        let mut health = active_definitions(&state)
            .into_iter()
            .map(|definition| {
                let mut active_invocation_ids = state
                    .invocations
                    .values()
                    .filter(|invocation| {
                        invocation.definition_id == definition.managed_agent_id
                            && invocation.status.is_active()
                    })
                    .map(|invocation| invocation.invocation_id.clone())
                    .collect::<Vec<_>>();
                active_invocation_ids.sort();
                let reconciliation_required = state.invocations.values().any(|invocation| {
                    invocation.definition_id == definition.managed_agent_id
                        && invocation.status == ManagedAgentInvocationStatus::ReconciliationRequired
                }) || state.outbox.values().any(|effect| {
                    effect.status == FencedEffectStatus::ReconciliationRequired
                        && state
                            .invocations
                            .get(&effect.invocation_id)
                            .is_some_and(|invocation| {
                                invocation.definition_id == definition.managed_agent_id
                            })
                });
                let consecutive_failures = state
                    .consecutive_failures
                    .get(&definition.managed_agent_id)
                    .copied()
                    .unwrap_or_default();
                let status = if reconciliation_required {
                    ManagedAgentHealthStatus::ReconciliationRequired
                } else if consecutive_failures >= definition.health_policy.max_consecutive_failures
                {
                    ManagedAgentHealthStatus::CircuitOpen
                } else if consecutive_failures > 0 {
                    ManagedAgentHealthStatus::Degraded
                } else {
                    ManagedAgentHealthStatus::Healthy
                };
                ManagedAgentHealth {
                    managed_agent_id: definition.managed_agent_id,
                    revision: definition.revision,
                    status,
                    consecutive_failures,
                    max_consecutive_failures: definition.health_policy.max_consecutive_failures,
                    active_invocation_ids,
                }
            })
            .collect::<Vec<_>>();
        health.sort_by(|left, right| left.managed_agent_id.cmp(&right.managed_agent_id));
        Ok(health)
    }

    pub fn trigger_manual(
        &self,
        managed_agent_id: &str,
        request_id: &str,
        now_ms: u64,
    ) -> Result<ManagedAgentInvocation, String> {
        if request_id.trim().is_empty() {
            return Err("managed Agent manual trigger requires request_id".to_string());
        }
        self.mutate("managed_agent.invocation.manual.v1", |state| {
            let definition = active_definition(state, managed_agent_id)?;
            if !definition.enabled {
                return Err("managed Agent definition is disabled".to_string());
            }
            if !matches!(definition.trigger, ManagedAgentTrigger::Manual) {
                return Err("managed Agent definition does not accept manual triggers".to_string());
            }
            accept_invocation(
                state,
                &definition,
                ManagedAgentInvocationTrigger::Manual {
                    request_id: request_id.to_string(),
                },
                format!("manual:{request_id}"),
                now_ms,
            )
        })
    }

    /// Normalize one external event into zero or more durable invocations.
    /// Connector code never decides an overlap policy or invokes a Run.
    pub fn accept_event(
        &self,
        event: ManagedAgentTriggerEvent,
        now_ms: u64,
    ) -> Result<ManagedAgentDispatchReport, String> {
        event.validate().map_err(|error| error.to_string())?;
        self.mutate("managed_agent.event.accepted.v1", |state| {
            let mut accepted = Vec::new();
            let mut suppressed = Vec::new();
            let mut rejected = Vec::new();
            let definitions = active_definitions(state);
            for definition in definitions {
                let ManagedAgentTrigger::Event(trigger) = &definition.trigger else {
                    continue;
                };
                if !definition.enabled || !event_matches(trigger, &event, now_ms) {
                    continue;
                }
                let trigger_identity = format!("event:{}", event.idempotency_key);
                let invocation_id = deterministic_invocation_id(
                    &definition.managed_agent_id,
                    definition.revision,
                    &trigger_identity,
                );
                if let Some(existing) = state.invocations.get(&invocation_id) {
                    accepted.push(existing.clone());
                    continue;
                }
                if !event_sequence_is_accepted(state, &definition, trigger, &event) {
                    rejected.push(format!(
                        "{}@{}: source event sequence is older than the accepted partition cursor",
                        definition.managed_agent_id, definition.revision
                    ));
                    continue;
                }
                match accept_invocation(
                    state,
                    &definition,
                    ManagedAgentInvocationTrigger::Event {
                        event_id: event.event_id.clone(),
                        source_id: event.source_id.clone(),
                        source_kind: event.source_kind.clone(),
                        event_type: event.event_type.clone(),
                        payload_ref: event.payload_ref.clone(),
                        payload_digest: event.payload_digest.clone(),
                    },
                    trigger_identity,
                    now_ms,
                ) {
                    Ok(invocation)
                        if invocation.status == ManagedAgentInvocationStatus::SuppressedOverlap =>
                    {
                        record_event_sequence(state, &definition, trigger, &event);
                        suppressed.push(invocation);
                    }
                    Ok(invocation) => {
                        record_event_sequence(state, &definition, trigger, &event);
                        accepted.push(invocation);
                    }
                    Err(error) => rejected.push(format!(
                        "{}@{}: {error}",
                        definition.managed_agent_id, definition.revision
                    )),
                }
            }
            Ok(ManagedAgentDispatchReport {
                accepted,
                suppressed,
                rejected,
            })
        })
    }

    /// Claim due schedule occurrences.  A timer host may call this often; an
    /// empty tick is read-only and produces no event churn.
    pub fn accept_due_schedules(&self, now_ms: u64) -> Result<ManagedAgentDispatchReport, String> {
        self.mutate("managed_agent.schedule.accepted.v1", |state| {
            let mut accepted = Vec::new();
            let mut suppressed = Vec::new();
            let mut rejected = Vec::new();
            for definition in active_definitions(state) {
                let ManagedAgentTrigger::Schedule { trigger } = &definition.trigger else {
                    continue;
                };
                if !definition.enabled {
                    continue;
                }
                let key = definition_key(&definition.managed_agent_id, definition.revision);
                let Some(due_at_ms) = state.schedule_next_at_ms.get(&key).copied() else {
                    rejected.push(format!("{}: schedule cursor is missing", key));
                    continue;
                };
                if due_at_ms > now_ms {
                    continue;
                }
                let next = next_schedule_after(trigger, due_at_ms)?;
                match next {
                    Some(next_at_ms) => {
                        state.schedule_next_at_ms.insert(key, next_at_ms);
                    }
                    None => {
                        state.schedule_next_at_ms.remove(&key);
                    }
                }
                match accept_invocation(
                    state,
                    &definition,
                    ManagedAgentInvocationTrigger::Schedule { due_at_ms },
                    format!("schedule:{due_at_ms}"),
                    now_ms,
                ) {
                    Ok(invocation)
                        if invocation.status == ManagedAgentInvocationStatus::SuppressedOverlap =>
                    {
                        suppressed.push(invocation);
                    }
                    Ok(invocation) => accepted.push(invocation),
                    Err(error) => {
                        rejected.push(format!("{}: {error}", definition.managed_agent_id))
                    }
                }
            }
            Ok(ManagedAgentDispatchReport {
                accepted,
                suppressed,
                rejected,
            })
        })
    }

    /// Claim work using a monotonically increasing per-invocation fence. A
    /// stale dispatcher can retain a process, but it cannot later start or
    /// complete this invocation after another dispatcher has reclaimed it.
    pub fn claim_ready(
        &self,
        dispatcher_id: &str,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<ManagedAgentInvocation>, String> {
        if dispatcher_id.trim().is_empty() || limit == 0 {
            return Err(
                "managed Agent claim requires dispatcher_id and positive limit".to_string(),
            );
        }
        self.mutate("managed_agent.invocation.claimed.v1", |state| {
            let mut claimed = Vec::new();
            let ids = state.invocations.keys().cloned().collect::<Vec<_>>();
            for invocation_id in ids {
                if claimed.len() >= limit {
                    break;
                }
                let Some(invocation) = state.invocations.get_mut(&invocation_id) else {
                    continue;
                };
                let ready = invocation.ready_at_ms.unwrap_or(invocation.created_at_ms);
                if !matches!(
                    invocation.status,
                    ManagedAgentInvocationStatus::Pending
                        | ManagedAgentInvocationStatus::RetryScheduled
                ) || ready > now_ms
                {
                    continue;
                }
                invocation.status = ManagedAgentInvocationStatus::Claimed;
                invocation.fence_generation = invocation.fence_generation.saturating_add(1);
                invocation.claimed_by = Some(dispatcher_id.to_string());
                invocation.ready_at_ms = None;
                claimed.push(invocation.clone());
            }
            Ok(claimed)
        })
    }

    pub fn start_invocation(
        &self,
        invocation_id: &str,
        dispatcher_id: &str,
        fence_generation: u64,
        execution_ref: String,
        now_ms: u64,
    ) -> Result<ManagedAgentInvocation, String> {
        self.mutate("managed_agent.invocation.started.v1", |state| {
            let invocation =
                invocation_for_fence(state, invocation_id, dispatcher_id, fence_generation)?;
            if invocation.status != ManagedAgentInvocationStatus::Claimed {
                return Err("managed Agent invocation is not claimed".to_string());
            }
            invocation.status = ManagedAgentInvocationStatus::Running;
            invocation.execution_ref = Some(execution_ref.clone());
            invocation.started_at_ms = Some(now_ms);
            Ok(invocation.clone())
        })
    }

    /// Stop invocations whose declared maximum age elapsed.  The status is
    /// reconciliation-required rather than failed/retryable because the
    /// process may still have performed an external effect after Runtime
    /// stopped observing it.
    pub fn enforce_run_health(&self, now_ms: u64) -> Result<Vec<ManagedAgentInvocation>, String> {
        self.mutate("managed_agent.health.timeout.v1", |state| {
            let mut affected = Vec::new();
            let ids = state.invocations.keys().cloned().collect::<Vec<_>>();
            for invocation_id in ids {
                let Some(invocation) = state.invocations.get(&invocation_id).cloned() else {
                    continue;
                };
                if invocation.status != ManagedAgentInvocationStatus::Running {
                    continue;
                }
                let definition = definition_at(
                    state,
                    &invocation.definition_id,
                    invocation.definition_revision,
                )?;
                let Some(max_run_age_ms) = definition.health_policy.max_run_age_ms else {
                    continue;
                };
                let started_at_ms = invocation.started_at_ms.unwrap_or(invocation.created_at_ms);
                if now_ms.saturating_sub(started_at_ms) <= max_run_age_ms {
                    continue;
                }
                if let Some(invocation) = state.invocations.get_mut(&invocation_id) {
                    invocation.status = ManagedAgentInvocationStatus::ReconciliationRequired;
                    invocation.error = Some(format!(
                        "Managed Agent execution exceeded health max_run_age_ms={max_run_age_ms}; reconcile before any retry"
                    ));
                    affected.push(invocation.clone());
                }
            }
            Ok(affected)
        })
    }

    pub fn reset_health(&self, managed_agent_id: &str) -> Result<ManagedAgentHealth, String> {
        self.mutate("managed_agent.health.reset.v1", |state| {
            let definition = active_definition(state, managed_agent_id)?;
            state
                .consecutive_failures
                .insert(managed_agent_id.to_string(), 0);
            Ok(ManagedAgentHealth {
                managed_agent_id: definition.managed_agent_id,
                revision: definition.revision,
                status: ManagedAgentHealthStatus::Healthy,
                consecutive_failures: 0,
                max_consecutive_failures: definition.health_policy.max_consecutive_failures,
                active_invocation_ids: state
                    .invocations
                    .values()
                    .filter(|invocation| {
                        invocation.definition_id == managed_agent_id
                            && invocation.status.is_active()
                    })
                    .map(|invocation| invocation.invocation_id.clone())
                    .collect(),
            })
        })
    }

    pub fn complete_invocation(
        &self,
        invocation_id: &str,
        dispatcher_id: &str,
        fence_generation: u64,
        succeeded: bool,
        now_ms: u64,
        execution_ref: Option<String>,
        evidence_refs: Vec<String>,
        error: Option<String>,
    ) -> Result<ManagedAgentInvocation, String> {
        self.mutate("managed_agent.invocation.completed.v1", |state| {
            let (definition_id, definition_revision) = {
                let invocation =
                    invocation_for_fence(state, invocation_id, dispatcher_id, fence_generation)?;
                if invocation.status != ManagedAgentInvocationStatus::Running {
                    return Err("managed Agent invocation is not running".to_string());
                }
                (
                    invocation.definition_id.clone(),
                    invocation.definition_revision,
                )
            };
            let definition = definition_at(state, &definition_id, definition_revision)?;
            let completed = {
                let invocation =
                    invocation_for_fence(state, invocation_id, dispatcher_id, fence_generation)?;
                if let Some(execution_ref) = execution_ref.as_ref() {
                    invocation.execution_ref = Some(execution_ref.clone());
                }
                invocation.evidence_refs = sorted_unique(evidence_refs.clone());
                invocation.error = error.clone();
                if succeeded {
                    invocation.status = ManagedAgentInvocationStatus::Completed;
                } else if invocation.attempt_no < definition.retry_policy.max_attempts {
                    invocation.attempt_no = invocation.attempt_no.saturating_add(1);
                    invocation.status = ManagedAgentInvocationStatus::RetryScheduled;
                    invocation.ready_at_ms = Some(now_ms.saturating_add(retry_backoff_ms(
                        &definition.retry_policy,
                        invocation.attempt_no,
                    )));
                } else {
                    invocation.status = ManagedAgentInvocationStatus::Failed;
                }
                invocation.clone()
            };
            if succeeded {
                state.consecutive_failures.insert(definition_id, 0);
            } else if completed.status == ManagedAgentInvocationStatus::Failed {
                let failures = state.consecutive_failures.entry(definition_id).or_default();
                *failures = failures.saturating_add(1);
            }
            Ok(completed)
        })
    }

    /// Persist an external effect intent before its adapter is allowed to run.
    /// The actual adapter must claim and complete this record using the same
    /// invocation fence; missing receipts become reconciliation work rather
    /// than an unsafe blind retry.
    pub fn enqueue_effect(
        &self,
        invocation_id: &str,
        dispatcher_id: &str,
        fence_generation: u64,
        effect_id: &str,
        effect_kind: String,
        idempotency_key: String,
        request_ref: String,
        now_ms: u64,
    ) -> Result<FencedEffectOutboxRecord, String> {
        if effect_id.trim().is_empty()
            || effect_kind.trim().is_empty()
            || idempotency_key.trim().is_empty()
            || request_ref.trim().is_empty()
        {
            return Err(
                "fenced effect requires non-empty identity, kind, idempotency key and request ref"
                    .to_string(),
            );
        }
        self.mutate("managed_agent.effect.enqueued.v1", |state| {
            let attempt_no = {
                let invocation =
                    invocation_for_fence(state, invocation_id, dispatcher_id, fence_generation)?;
                if invocation.status != ManagedAgentInvocationStatus::Running {
                    return Err("fenced effect requires a running invocation".to_string());
                }
                invocation.attempt_no
            };
            let key = format!("{invocation_id}:{effect_id}:{fence_generation}");
            if let Some(existing) = state.outbox.get(&key) {
                return Ok(existing.clone());
            }
            let record = FencedEffectOutboxRecord {
                effect_id: effect_id.to_string(),
                invocation_id: invocation_id.to_string(),
                attempt_no,
                fence_generation,
                effect_kind: effect_kind.clone(),
                idempotency_key: idempotency_key.clone(),
                request_ref: request_ref.clone(),
                status: FencedEffectStatus::Pending,
                created_at_ms: now_ms,
                claimed_by: None,
                receipt_ref: None,
                error: None,
            };
            state.outbox.insert(key, record.clone());
            Ok(record)
        })
    }

    pub fn claim_effect(
        &self,
        invocation_id: &str,
        effect_id: &str,
        fence_generation: u64,
        dispatcher_id: &str,
    ) -> Result<FencedEffectOutboxRecord, String> {
        self.mutate("managed_agent.effect.claimed.v1", |state| {
            let key = format!("{invocation_id}:{effect_id}:{fence_generation}");
            let invocation = state
                .invocations
                .get(invocation_id)
                .ok_or_else(|| "managed Agent invocation not found".to_string())?;
            if invocation.fence_generation != fence_generation
                || invocation.claimed_by.as_deref() != Some(dispatcher_id)
                || invocation.status != ManagedAgentInvocationStatus::Running
            {
                return Err("stale dispatcher fence cannot claim effect".to_string());
            }
            let record = state
                .outbox
                .get_mut(&key)
                .ok_or_else(|| "fenced effect not found".to_string())?;
            if record.status != FencedEffectStatus::Pending {
                return Err("fenced effect is not pending".to_string());
            }
            record.status = FencedEffectStatus::Claimed;
            record.claimed_by = Some(dispatcher_id.to_string());
            Ok(record.clone())
        })
    }

    pub fn complete_effect(
        &self,
        invocation_id: &str,
        effect_id: &str,
        fence_generation: u64,
        dispatcher_id: &str,
        receipt_ref: String,
    ) -> Result<FencedEffectOutboxRecord, String> {
        self.mutate("managed_agent.effect.completed.v1", |state| {
            let key = format!("{invocation_id}:{effect_id}:{fence_generation}");
            let invocation = state
                .invocations
                .get(invocation_id)
                .ok_or_else(|| "managed Agent invocation not found".to_string())?;
            if invocation.fence_generation != fence_generation
                || invocation.claimed_by.as_deref() != Some(dispatcher_id)
            {
                return Err("stale dispatcher fence cannot complete effect".to_string());
            }
            let record = state
                .outbox
                .get_mut(&key)
                .ok_or_else(|| "fenced effect not found".to_string())?;
            if record.status != FencedEffectStatus::Claimed
                || record.claimed_by.as_deref() != Some(dispatcher_id)
            {
                return Err("fenced effect is not claimed by this dispatcher".to_string());
            }
            record.status = FencedEffectStatus::Completed;
            record.receipt_ref = Some(receipt_ref.clone());
            Ok(record.clone())
        })
    }

    /// Mark an uncertain adapter outcome as reconciliation work.  We never
    /// return it to Pending because a transport error may have occurred after
    /// the remote side accepted the effect.
    pub fn mark_effect_reconciliation_required(
        &self,
        invocation_id: &str,
        effect_id: &str,
        fence_generation: u64,
        dispatcher_id: &str,
        error: String,
    ) -> Result<FencedEffectOutboxRecord, String> {
        self.mutate("managed_agent.effect.reconciliation_required.v1", |state| {
            let key = format!("{invocation_id}:{effect_id}:{fence_generation}");
            let invocation = state
                .invocations
                .get(invocation_id)
                .ok_or_else(|| "managed Agent invocation not found".to_string())?;
            if invocation.fence_generation != fence_generation
                || invocation.claimed_by.as_deref() != Some(dispatcher_id)
            {
                return Err("stale dispatcher fence cannot reconcile effect".to_string());
            }
            let record = state
                .outbox
                .get_mut(&key)
                .ok_or_else(|| "fenced effect not found".to_string())?;
            if record.claimed_by.as_deref() != Some(dispatcher_id)
                || !matches!(
                    record.status,
                    FencedEffectStatus::Claimed | FencedEffectStatus::Pending
                )
            {
                return Err("fenced effect is not controlled by this dispatcher".to_string());
            }
            record.status = FencedEffectStatus::ReconciliationRequired;
            record.error = Some(error.clone());
            Ok(record.clone())
        })
    }

    /// After a crash, a claimed-but-not-started invocation may be returned to
    /// Pending. A Running invocation or claimed effect is deliberately not
    /// replayed: its external result is uncertain and must be reconciled.
    pub fn recover(&self, now_ms: u64) -> Result<Vec<ManagedAgentInvocation>, String> {
        self.mutate("managed_agent.dispatcher.recovered.v1", |state| {
            let mut affected = Vec::new();
            for invocation in state.invocations.values_mut() {
                match invocation.status {
                    ManagedAgentInvocationStatus::Claimed => {
                        invocation.status = ManagedAgentInvocationStatus::Pending;
                        invocation.claimed_by = None;
                        invocation.ready_at_ms = Some(now_ms);
                        affected.push(invocation.clone());
                    }
                    ManagedAgentInvocationStatus::Running => {
                        invocation.status = ManagedAgentInvocationStatus::ReconciliationRequired;
                        invocation.error = Some(
                            "Runtime restarted while Managed Agent invocation was running; external effects require reconciliation"
                                .to_string(),
                        );
                        affected.push(invocation.clone());
                    }
                    _ => {}
                }
            }
            for effect in state.outbox.values_mut() {
                if effect.status == FencedEffectStatus::Claimed {
                    effect.status = FencedEffectStatus::ReconciliationRequired;
                    effect.error = Some(
                        "Runtime restarted after effect claim; receipt must be reconciled before retry"
                            .to_string(),
                    );
                }
            }
            Ok(affected)
        })
    }

    pub fn projection(&self) -> Result<serde_json::Value, String> {
        let state = self.state()?;
        let definitions = self.definitions()?;
        let invocations = state.invocations.into_values().collect::<Vec<_>>();
        let outbox = state.outbox.into_values().collect::<Vec<_>>();
        Ok(serde_json::json!({
            "kind": "runtime.managed_agents",
            "stream_id": self.stream_id,
            "definitions": definitions,
            "invocations": invocations,
            "effects": outbox,
            "health": self.health()?,
        }))
    }

    fn state(&self) -> Result<ManagedAgentState, String> {
        load_state(&self.event_store, &self.stream_id)
    }

    fn mutate<T: Clone>(
        &self,
        event_kind: &str,
        mut operation: impl FnMut(&mut ManagedAgentState) -> Result<T, String>,
    ) -> Result<T, String> {
        let _local_guard = self
            .mutation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for attempt in 0..3 {
            let previous = self.state()?;
            let mut next = previous.clone();
            let result = operation(&mut next)?;
            if next == previous {
                return Ok(result);
            }
            let revision = self
                .event_store
                .stream_revision(&self.stream_id)
                .map_err(|error| error.to_string())?;
            let input = RuntimeEventInput {
                stream_id: self.stream_id.clone(),
                scope: RuntimeEventScope::ManagedAgent,
                kind: event_kind.to_string(),
                status: Some("committed".to_string()),
                actor: Some("runtime.managed_agent_dispatcher".to_string()),
                refs: Vec::new(),
                payload: serde_json::to_value(ManagedAgentStateEvent { state: next })
                    .map_err(|error| error.to_string())?,
            };
            match self.event_store.append_batch_if_revision(
                self.stream_id.clone(),
                revision,
                format!(
                    "managed-agent:{}:{}:{attempt}",
                    self.stream_id,
                    revision.saturating_add(1)
                ),
                vec![RuntimeTransactionEventInput {
                    event: input,
                    idempotency_key: Some(format!(
                        "managed-agent-revision:{}",
                        revision.saturating_add(1)
                    )),
                    schema_version: 1,
                }],
            ) {
                Ok(_) => return Ok(result),
                Err(RuntimeEventStoreError::StaleRevision { .. }) if attempt < 2 => continue,
                Err(error) => return Err(error.to_string()),
            }
        }
        Err("managed Agent state retry budget exhausted".to_string())
    }
}

fn load_state(
    event_store: &RuntimeEventStore,
    stream_id: &str,
) -> Result<ManagedAgentState, String> {
    let mut events = event_store.list_stream(stream_id)?;
    events.sort_by_key(|event| event.sequence);
    Ok(events
        .into_iter()
        .rev()
        .find_map(|event| {
            (event.kind.starts_with("managed_agent."))
                .then_some(event.payload)
                .and_then(|payload| serde_json::from_value::<ManagedAgentStateEvent>(payload).ok())
                .map(|snapshot| snapshot.state)
        })
        .unwrap_or_default())
}

fn active_definition(
    state: &ManagedAgentState,
    id: &str,
) -> Result<ManagedAgentDefinition, String> {
    let revision = state
        .active_revisions
        .get(id)
        .copied()
        .ok_or_else(|| format!("managed Agent definition not found: {id}"))?;
    definition_at(state, id, revision)
}

fn definition_at(
    state: &ManagedAgentState,
    id: &str,
    revision: u64,
) -> Result<ManagedAgentDefinition, String> {
    state
        .definitions
        .get(id)
        .and_then(|revisions| revisions.get(&revision))
        .cloned()
        .ok_or_else(|| format!("managed Agent definition revision not found: {id}@{revision}"))
}

fn active_definitions(state: &ManagedAgentState) -> Vec<ManagedAgentDefinition> {
    state
        .active_revisions
        .iter()
        .filter_map(|(id, revision)| definition_at(state, id, *revision).ok())
        .collect()
}

fn accept_invocation(
    state: &mut ManagedAgentState,
    definition: &ManagedAgentDefinition,
    trigger: ManagedAgentInvocationTrigger,
    trigger_identity: String,
    now_ms: u64,
) -> Result<ManagedAgentInvocation, String> {
    let consecutive_failures = state
        .consecutive_failures
        .get(&definition.managed_agent_id)
        .copied()
        .unwrap_or_default();
    if consecutive_failures >= definition.health_policy.max_consecutive_failures {
        return Err(format!(
            "managed Agent health circuit is open after {consecutive_failures} consecutive failures; reset health or register a corrected revision"
        ));
    }
    let invocation_id = deterministic_invocation_id(
        &definition.managed_agent_id,
        definition.revision,
        &trigger_identity,
    );
    if let Some(existing) = state.invocations.get(&invocation_id) {
        return Ok(existing.clone());
    }
    let active = state
        .invocations
        .values()
        .filter(|invocation| {
            invocation.definition_id == definition.managed_agent_id && invocation.status.is_active()
        })
        .map(|invocation| invocation.invocation_id.clone())
        .collect::<Vec<_>>();
    let status = match definition.overlap_policy {
        ManagedAgentOverlapPolicy::Forbid if !active.is_empty() => {
            ManagedAgentInvocationStatus::SuppressedOverlap
        }
        ManagedAgentOverlapPolicy::AllowParallel { max_concurrent }
            if active.len() >= usize::from(max_concurrent) =>
        {
            ManagedAgentInvocationStatus::SuppressedOverlap
        }
        ManagedAgentOverlapPolicy::Replace if !active.is_empty() => {
            for active_id in active {
                if let Some(invocation) = state.invocations.get_mut(&active_id) {
                    invocation.status = ManagedAgentInvocationStatus::Cancelled;
                    invocation.error = Some("superseded by replace overlap policy".to_string());
                }
            }
            ManagedAgentInvocationStatus::Pending
        }
        _ => ManagedAgentInvocationStatus::Pending,
    };
    let invocation = ManagedAgentInvocation {
        invocation_id: invocation_id.clone(),
        definition_id: definition.managed_agent_id.clone(),
        definition_revision: definition.revision,
        trigger,
        status,
        attempt_no: 1,
        fence_generation: 0,
        created_at_ms: now_ms,
        ready_at_ms: Some(now_ms),
        claimed_by: None,
        execution_ref: None,
        started_at_ms: None,
        error: None,
        evidence_refs: Vec::new(),
    };
    state.invocations.insert(invocation_id, invocation.clone());
    Ok(invocation)
}

fn invocation_for_fence<'a>(
    state: &'a mut ManagedAgentState,
    invocation_id: &str,
    dispatcher_id: &str,
    fence_generation: u64,
) -> Result<&'a mut ManagedAgentInvocation, String> {
    let invocation = state
        .invocations
        .get_mut(invocation_id)
        .ok_or_else(|| "managed Agent invocation not found".to_string())?;
    if invocation.fence_generation != fence_generation
        || invocation.claimed_by.as_deref() != Some(dispatcher_id)
    {
        return Err("stale dispatcher fence".to_string());
    }
    Ok(invocation)
}

fn event_matches(
    trigger: &ManagedAgentEventTrigger,
    event: &ManagedAgentTriggerEvent,
    now_ms: u64,
) -> bool {
    trigger.source_id == event.source_id
        && trigger.source_kind == event.source_kind
        && trigger.event_type == event.event_type
        && trigger
            .maximum_age_ms
            .is_none_or(|maximum| now_ms.saturating_sub(event.occurred_at_ms) <= maximum)
        && trigger.required_source_capabilities.iter().all(|required| {
            event
                .source_capabilities
                .iter()
                .any(|actual| actual == required)
        })
        && trigger.required_attributes.iter().all(|(key, expected)| {
            event
                .attributes
                .get(key)
                .is_some_and(|actual| actual == expected)
        })
}

fn event_sequence_key(
    definition: &ManagedAgentDefinition,
    event: &ManagedAgentTriggerEvent,
) -> String {
    format!(
        "{}@{}:{}:{}",
        definition.managed_agent_id, definition.revision, event.source_id, event.subject
    )
}

fn event_sequence_is_accepted(
    state: &ManagedAgentState,
    definition: &ManagedAgentDefinition,
    trigger: &ManagedAgentEventTrigger,
    event: &ManagedAgentTriggerEvent,
) -> bool {
    if trigger.out_of_order_policy != ManagedAgentEventOrderPolicy::RejectOlderSequence {
        return true;
    }
    let Some(sequence) = event.source_sequence else {
        return false;
    };
    state
        .event_sequences
        .get(&event_sequence_key(definition, event))
        .is_none_or(|previous| sequence >= *previous)
}

fn record_event_sequence(
    state: &mut ManagedAgentState,
    definition: &ManagedAgentDefinition,
    trigger: &ManagedAgentEventTrigger,
    event: &ManagedAgentTriggerEvent,
) {
    if trigger.out_of_order_policy != ManagedAgentEventOrderPolicy::RejectOlderSequence {
        return;
    }
    if let Some(sequence) = event.source_sequence {
        state
            .event_sequences
            .entry(event_sequence_key(definition, event))
            .and_modify(|previous| *previous = (*previous).max(sequence))
            .or_insert(sequence);
    }
}

fn retry_backoff_ms(
    policy: &harness_contract::managed_agent::ManagedAgentRetryPolicy,
    attempt_no: u16,
) -> u64 {
    let shift = u32::from(attempt_no.saturating_sub(2)).min(20);
    policy
        .initial_backoff_ms
        .saturating_mul(1_u64 << shift)
        .min(policy.max_backoff_ms)
}

fn definition_key(id: &str, revision: u64) -> String {
    format!("{id}@{revision}")
}

fn deterministic_invocation_id(id: &str, revision: u64, trigger_identity: &str) -> String {
    let digest = Sha256::digest(format!("{id}\n{revision}\n{trigger_identity}").as_bytes());
    format!("managed-invocation:{id}:{revision}:{digest:x}")
}

fn sorted_unique(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    values
}

fn next_schedule_after(
    trigger: &harness_contract::mission::ScheduleTrigger,
    after_ms: u64,
) -> Result<Option<u64>, String> {
    use harness_contract::mission::ScheduleTrigger;
    match trigger {
        ScheduleTrigger::At { at_ms } => Ok((*at_ms > after_ms).then_some(*at_ms)),
        ScheduleTrigger::Interval { every_ms } => {
            if *every_ms == 0 {
                return Err("managed Agent schedule interval must be positive".to_string());
            }
            let next = after_ms
                .checked_div(*every_ms)
                .and_then(|index| index.checked_add(1))
                .and_then(|index| index.checked_mul(*every_ms))
                .ok_or_else(|| "managed Agent interval next occurrence overflow".to_string())?;
            Ok(Some(next))
        }
        ScheduleTrigger::Cron {
            expression,
            timezone,
        } => {
            let schedule = Schedule::from_str(expression)
                .map_err(|error| format!("invalid managed Agent cron: {error}"))?;
            let timezone = Tz::from_str(timezone)
                .map_err(|error| format!("invalid managed Agent cron timezone: {error}"))?;
            let seconds = i64::try_from(after_ms / 1_000)
                .map_err(|_| "managed Agent cron timestamp exceeds i64".to_string())?;
            let nanos = u32::try_from((after_ms % 1_000) * 1_000_000)
                .map_err(|_| "managed Agent cron nanoseconds overflow".to_string())?;
            let after = DateTime::<Utc>::from_timestamp(seconds, nanos)
                .ok_or_else(|| "invalid managed Agent cron timestamp".to_string())?
                .with_timezone(&timezone);
            let next = schedule.after(&after).next().map(|value| {
                u64::try_from(value.with_timezone(&Utc).timestamp_millis()).unwrap_or(u64::MAX)
            });
            Ok(next)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::agent::{AgentDefinitionId, DefinitionScope, RevisionSelector};
    use harness_contract::managed_agent::{ManagedAgentHealthPolicy, ManagedAgentRetryPolicy};

    fn dispatcher() -> ManagedAgentDispatcher {
        ManagedAgentDispatcher::event_sourced(
            Arc::new(RuntimeEventStore::try_open_in_memory().expect("store")),
            "workspace-test",
        )
        .expect("dispatcher")
    }

    fn definition(trigger: ManagedAgentTrigger) -> ManagedAgentDefinition {
        ManagedAgentDefinition {
            managed_agent_id: "workspace/cowd/research-watch".to_string(),
            revision: 1,
            target: harness_contract::managed_agent::ManagedAgentTarget::Agent {
                definition_id: AgentDefinitionId::new(
                    DefinitionScope::Workspace,
                    "cowd/researcher",
                )
                .expect("definition id"),
                selector: RevisionSelector::LatestApprovedStable,
            },
            trigger,
            session_id: "managed-session".to_string(),
            objective: "inspect durable evidence".to_string(),
            acceptance: vec!["evidence".to_string()],
            permission_lease: "read_only".to_string(),
            model_lease: "default".to_string(),
            granted_capabilities: vec![harness_contract::agent::AgentCapability::Read],
            allowed_tool_contract_refs: Vec::new(),
            allowed_skill_refs: Vec::new(),
            resource_scopes: vec!["workspace".to_string()],
            overlap_policy: ManagedAgentOverlapPolicy::Forbid,
            retry_policy: ManagedAgentRetryPolicy::default(),
            health_policy: ManagedAgentHealthPolicy::default(),
            enabled: true,
        }
    }

    #[test]
    fn event_trigger_is_deduplicated_and_overlap_is_durable() {
        let dispatcher = dispatcher();
        dispatcher
            .register_definition(
                definition(ManagedAgentTrigger::Event(ManagedAgentEventTrigger {
                    source_id: "feishu".to_string(),
                    source_kind: "channel".to_string(),
                    event_type: "message".to_string(),
                    required_source_capabilities: Vec::new(),
                    required_attributes: BTreeMap::new(),
                    maximum_age_ms: Some(10_000),
                    out_of_order_policy: ManagedAgentEventOrderPolicy::AcceptAny,
                })),
                1,
            )
            .expect("definition");
        let event = ManagedAgentTriggerEvent {
            event_id: "event-1".to_string(),
            source_id: "feishu".to_string(),
            source_kind: "channel".to_string(),
            event_type: "message".to_string(),
            subject: "conversation-1".to_string(),
            payload_ref: "attachment:event-1".to_string(),
            payload_digest: "sha256:event-1".to_string(),
            occurred_at_ms: 2,
            source_sequence: Some(1),
            idempotency_key: "feishu:message:1".to_string(),
            source_capabilities: Vec::new(),
            attributes: BTreeMap::new(),
            trace_refs: Vec::new(),
        };
        let first = dispatcher.accept_event(event.clone(), 3).expect("first");
        assert_eq!(first.accepted.len(), 1);
        let replay = dispatcher.accept_event(event, 4).expect("replay");
        assert_eq!(
            replay.accepted[0].invocation_id,
            first.accepted[0].invocation_id
        );
        let second = ManagedAgentTriggerEvent {
            event_id: "event-2".to_string(),
            idempotency_key: "feishu:message:2".to_string(),
            source_sequence: Some(2),
            ..ManagedAgentTriggerEvent {
                event_id: "event-1".to_string(),
                source_id: "feishu".to_string(),
                source_kind: "channel".to_string(),
                event_type: "message".to_string(),
                subject: "conversation-1".to_string(),
                payload_ref: "attachment:event-1".to_string(),
                payload_digest: "sha256:event-1".to_string(),
                occurred_at_ms: 2,
                source_sequence: Some(1),
                idempotency_key: "feishu:message:1".to_string(),
                source_capabilities: Vec::new(),
                attributes: BTreeMap::new(),
                trace_refs: Vec::new(),
            }
        };
        let suppressed = dispatcher.accept_event(second, 5).expect("overlap");
        assert_eq!(suppressed.suppressed.len(), 1);
        assert_eq!(
            suppressed.suppressed[0].status,
            ManagedAgentInvocationStatus::SuppressedOverlap
        );
    }

    #[test]
    fn fence_and_effect_recovery_refuse_unsafe_replay() {
        let dispatcher = dispatcher();
        dispatcher
            .register_definition(definition(ManagedAgentTrigger::Manual), 1)
            .expect("definition");
        let invocation = dispatcher
            .trigger_manual("workspace/cowd/research-watch", "request-1", 2)
            .expect("manual");
        let claim = dispatcher
            .claim_ready("dispatcher-a", 3, 1)
            .expect("claim")
            .pop()
            .expect("claimed invocation");
        dispatcher
            .start_invocation(
                &claim.invocation_id,
                "dispatcher-a",
                claim.fence_generation,
                "run:1".to_string(),
                4,
            )
            .expect("start");
        dispatcher
            .enqueue_effect(
                &invocation.invocation_id,
                "dispatcher-a",
                claim.fence_generation,
                "send",
                "channel_send".to_string(),
                "effect-key".to_string(),
                "request:send".to_string(),
                4,
            )
            .expect("effect");
        dispatcher
            .claim_effect(
                &invocation.invocation_id,
                "send",
                claim.fence_generation,
                "dispatcher-a",
            )
            .expect("claim effect");
        let affected = dispatcher.recover(5).expect("recover");
        assert_eq!(affected.len(), 1);
        assert_eq!(
            dispatcher.invocations().expect("invocations")[0].status,
            ManagedAgentInvocationStatus::ReconciliationRequired
        );
        assert_eq!(
            dispatcher.outbox().expect("outbox")[0].status,
            FencedEffectStatus::ReconciliationRequired
        );
    }
}
