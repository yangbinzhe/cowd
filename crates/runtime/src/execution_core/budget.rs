use std::collections::BTreeMap;
use std::sync::Arc;

use harness_contract::context::{ChildExecutionBudgetReservation, ParentExecutionBudget};
use model_protocol::usage::TokenUsage;

use crate::runtime_event_store::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventScope,
    RuntimeEventStore, RuntimeEventStoreError, RuntimeTransactionEventInput,
};

const MAX_BUDGET_CAS_RETRIES: usize = 16;

/// Durable technical-capacity telemetry for one parent execution.
///
/// It records context/output capacity reservations and reconciliation across a
/// restart. It deliberately has no commercial model-rate dimension.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ParentExecutionBudgetSnapshot {
    pub reserved_tokens: u64,
    pub settled_tokens: u64,
    pub breached: bool,
}

#[derive(Debug, Clone, Copy)]
struct DurableProviderCharge {
    reserved_tokens: u64,
    actual_tokens: Option<u64>,
}

#[derive(Clone)]
pub struct ParentExecutionBudgetLedger {
    event_store: Arc<RuntimeEventStore>,
    budget: ParentExecutionBudget,
}

impl std::fmt::Debug for ParentExecutionBudgetLedger {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ParentExecutionBudgetLedger")
            .field("budget_id", &self.budget.budget_id)
            .field("revision", &self.budget.revision)
            .finish_non_exhaustive()
    }
}

pub struct DurableProviderBudgetReservation {
    ledger: ParentExecutionBudgetLedger,
    child: ChildExecutionBudgetReservation,
    reservation_id: String,
    pub reserved_tokens: u64,
    pub granted_output_tokens: u64,
    reconciled: bool,
}

impl ParentExecutionBudgetLedger {
    pub fn new(
        event_store: Arc<RuntimeEventStore>,
        budget: ParentExecutionBudget,
    ) -> Result<Self, String> {
        budget.validate().map_err(str::to_string)?;
        Ok(Self {
            event_store,
            budget,
        })
    }

    #[must_use]
    pub fn budget(&self) -> &ParentExecutionBudget {
        &self.budget
    }

    pub fn snapshot(&self) -> Result<ParentExecutionBudgetSnapshot, String> {
        let (_, _, charges) = self.load_state()?;
        Ok(snapshot_from_charges(charges.values()))
    }

    pub fn reserve_provider(
        &self,
        child: &ChildExecutionBudgetReservation,
        reservation_id: impl Into<String>,
        model: &str,
        input_reserve_tokens: u64,
        requested_output_tokens: u64,
    ) -> Result<DurableProviderBudgetReservation, String> {
        child.validate().map_err(str::to_string)?;
        self.validate_child(child)?;
        if model.trim().is_empty() || input_reserve_tokens == 0 || requested_output_tokens == 0 {
            return Err("provider capacity reservation input is incomplete".to_string());
        }
        let reservation_id = reservation_id.into();
        if reservation_id.trim().is_empty() {
            return Err("provider capacity reservation id is empty".to_string());
        }

        // Cache accounting can be reported separately from ordinary input.
        // Reserve two input copies so a provider's cache split does not evade
        // context/output telemetry. This remains an observation, not a
        // commercial gate or a cumulative execution kill switch.
        let reserved_tokens = input_reserve_tokens
            .saturating_mul(2)
            .saturating_add(requested_output_tokens);
        for _ in 0..MAX_BUDGET_CAS_RETRIES {
            let (stream_revision, created, charges) = self.load_state()?;
            if charges.contains_key(&reservation_id) {
                return Err(format!(
                    "provider capacity reservation `{reservation_id}` already exists"
                ));
            }
            let child_totals = self.child_snapshot(&child.lease_id)?;
            let mut events = Vec::with_capacity(2);
            if !created {
                events.push(self.created_event());
            }
            events.push(RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: self.stream_id(),
                    scope: RuntimeEventScope::Team,
                    kind: "execution_budget.provider_reserved".to_string(),
                    status: Some("reserved".to_string()),
                    actor: Some("runtime-provider-capacity".to_string()),
                    refs: Vec::new(),
                    payload: serde_json::json!({
                        "budget_id": self.budget.budget_id,
                        "budget_revision": self.budget.revision,
                        "child_lease_id": child.lease_id,
                        "reservation_id": reservation_id,
                        "model": model,
                        "reserved_tokens": reserved_tokens,
                        "child_target_tokens": child.max_tokens,
                        "child_tokens_before_reservation": child_totals.reserved_tokens,
                        "borrowed_from_parent_pool": child_totals
                            .reserved_tokens
                            .saturating_add(reserved_tokens) > child.max_tokens,
                        "capacity_prediction_exceeded": snapshot_from_charges(charges.values())
                            .reserved_tokens
                            .saturating_add(reserved_tokens) > self.budget.max_tokens,
                    }),
                },
                idempotency_key: Some(format!("provider-reserve:{reservation_id}")),
                schema_version: 1,
            });
            let request = AppendTransactionRequest {
                transaction_id: format!(
                    "budget:{}:reserve:{reservation_id}",
                    self.budget.budget_id
                ),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: self.stream_id(),
                    expected_revision: stream_revision,
                }],
                events,
            };
            match self.event_store.append_transaction(request) {
                Ok(_) => {
                    return Ok(DurableProviderBudgetReservation {
                        ledger: self.clone(),
                        child: child.clone(),
                        reservation_id,
                        reserved_tokens,
                        granted_output_tokens: requested_output_tokens,
                        reconciled: false,
                    });
                }
                Err(RuntimeEventStoreError::StaleRevision { .. }) => continue,
                Err(error) => return Err(error.to_string()),
            }
        }
        Err(format!(
            "execution capacity ledger `{}` remained contended after {MAX_BUDGET_CAS_RETRIES} CAS attempts",
            self.budget.budget_id
        ))
    }

    fn created_event(&self) -> RuntimeTransactionEventInput {
        RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: self.stream_id(),
                scope: RuntimeEventScope::Team,
                kind: "execution_budget.created".to_string(),
                status: Some("active".to_string()),
                actor: Some("runtime-provider-capacity".to_string()),
                refs: Vec::new(),
                payload: serde_json::to_value(&self.budget).unwrap_or_default(),
            },
            idempotency_key: Some(format!(
                "budget-created:{}:{}",
                self.budget.budget_id, self.budget.revision
            )),
            schema_version: 1,
        }
    }

    fn validate_child(&self, child: &ChildExecutionBudgetReservation) -> Result<(), String> {
        if !child.parent_budget.semantically_matches(&self.budget)
            || child.parent_budget_id != self.budget.budget_id
            || child.revision != self.budget.revision
            || child.deadline_at_ms != self.budget.deadline_at_ms
            || child.max_parallel != self.budget.max_parallel
            || child.max_tokens > self.budget.max_tokens
        {
            return Err("child execution budget does not match its immutable parent".to_string());
        }
        Ok(())
    }

    fn child_snapshot(
        &self,
        child_lease_id: &str,
    ) -> Result<ParentExecutionBudgetSnapshot, String> {
        let charges = self.load_charge_events(Some(child_lease_id))?;
        Ok(snapshot_from_charges(charges.values()))
    }

    fn load_state(&self) -> Result<(u64, bool, BTreeMap<String, DurableProviderCharge>), String> {
        let events = self.event_store.list_stream(&self.stream_id())?;
        let mut created = false;
        let mut durable_budget = None;
        let charges = replay_charges(&events, None, &mut created, &mut durable_budget)?;
        if let Some(durable_budget) = durable_budget {
            if !durable_budget.semantically_matches(&self.budget) {
                return Err(format!(
                    "execution budget `{}` durable contract differs from the current packet",
                    self.budget.budget_id
                ));
            }
        }
        let revision = events.last().map_or(0, |event| event.sequence);
        Ok((revision, created, charges))
    }

    fn load_charge_events(
        &self,
        child_lease_id: Option<&str>,
    ) -> Result<BTreeMap<String, DurableProviderCharge>, String> {
        let events = self.event_store.list_stream(&self.stream_id())?;
        replay_charges(&events, child_lease_id, &mut false, &mut None)
    }

    fn stream_id(&self) -> String {
        format!("execution-budget:{}", self.budget.budget_id)
    }
}

impl DurableProviderBudgetReservation {
    pub fn reconcile(&mut self, usage: TokenUsage) -> Result<(), String> {
        if self.reconciled {
            return Ok(());
        }
        let actual_tokens = u64::from(usage.input_tokens)
            .saturating_add(u64::from(usage.output_tokens))
            .saturating_add(u64::from(usage.cache_creation_input_tokens))
            .saturating_add(u64::from(usage.cache_read_input_tokens));
        let missing_usage = actual_tokens == 0;
        for _ in 0..MAX_BUDGET_CAS_RETRIES {
            let stream_revision = self
                .ledger
                .event_store
                .stream_revision(&self.ledger.stream_id())
                .map_err(|error| error.to_string())?;
            let request = AppendTransactionRequest {
                transaction_id: format!(
                    "budget:{}:reconcile:{}",
                    self.child.parent_budget_id, self.reservation_id
                ),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: self.ledger.stream_id(),
                    expected_revision: stream_revision,
                }],
                events: vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: self.ledger.stream_id(),
                        scope: RuntimeEventScope::Team,
                        kind: if missing_usage {
                            "execution_budget.usage_missing".to_string()
                        } else {
                            "execution_budget.provider_reconciled".to_string()
                        },
                        status: Some(if missing_usage {
                            "missing".to_string()
                        } else if actual_tokens <= self.reserved_tokens {
                            "settled".to_string()
                        } else {
                            "breached".to_string()
                        }),
                        actor: Some("runtime-provider-capacity".to_string()),
                        refs: Vec::new(),
                        payload: serde_json::json!({
                            "budget_id": self.child.parent_budget_id,
                            "budget_revision": self.child.revision,
                            "child_lease_id": self.child.lease_id,
                            "reservation_id": self.reservation_id,
                            "actual_tokens": actual_tokens,
                        }),
                    },
                    idempotency_key: Some(format!("provider-reconcile:{}", self.reservation_id)),
                    schema_version: 1,
                }],
            };
            match self.ledger.event_store.append_transaction(request) {
                Ok(_) => {
                    self.reconciled = true;
                    return Ok(());
                }
                Err(RuntimeEventStoreError::StaleRevision { .. }) => continue,
                Err(error) => return Err(error.to_string()),
            }
        }
        Err("provider capacity reconciliation remained CAS-contended".to_string())
    }
}

fn replay_charges(
    events: &[crate::DurableRuntimeEvent],
    child_lease_id: Option<&str>,
    created: &mut bool,
    durable_budget: &mut Option<ParentExecutionBudget>,
) -> Result<BTreeMap<String, DurableProviderCharge>, String> {
    let mut charges = BTreeMap::new();
    for event in events {
        match event.kind.as_str() {
            "execution_budget.created" => {
                *created = true;
                *durable_budget = Some(serde_json::from_value(event.payload.clone()).map_err(
                    |error| format!("invalid durable parent execution budget: {error}"),
                )?);
            }
            "execution_budget.provider_reserved" => {
                if child_lease_id.is_some_and(|expected| {
                    event
                        .payload
                        .get("child_lease_id")
                        .and_then(serde_json::Value::as_str)
                        != Some(expected)
                }) {
                    continue;
                }
                let reservation_id = required_string(&event.payload, "reservation_id")?;
                charges.insert(
                    reservation_id,
                    DurableProviderCharge {
                        reserved_tokens: required_u64(&event.payload, "reserved_tokens")?,
                        actual_tokens: None,
                    },
                );
            }
            "execution_budget.provider_reconciled" => {
                if child_lease_id.is_some_and(|expected| {
                    event
                        .payload
                        .get("child_lease_id")
                        .and_then(serde_json::Value::as_str)
                        != Some(expected)
                }) {
                    continue;
                }
                let reservation_id = required_string(&event.payload, "reservation_id")?;
                let charge = charges.get_mut(&reservation_id).ok_or_else(|| {
                    format!("budget reconciliation `{reservation_id}` has no reservation")
                })?;
                charge.actual_tokens = Some(required_u64(&event.payload, "actual_tokens")?);
            }
            _ => {}
        }
    }
    Ok(charges)
}

fn snapshot_from_charges<'a>(
    charges: impl IntoIterator<Item = &'a DurableProviderCharge>,
) -> ParentExecutionBudgetSnapshot {
    let mut snapshot = ParentExecutionBudgetSnapshot::default();
    for charge in charges {
        let tokens = charge.actual_tokens.unwrap_or(charge.reserved_tokens);
        snapshot.reserved_tokens = snapshot.reserved_tokens.saturating_add(tokens);
        if charge.actual_tokens.is_some() {
            snapshot.settled_tokens = snapshot.settled_tokens.saturating_add(tokens);
        }
        snapshot.breached |= tokens > charge.reserved_tokens;
    }
    snapshot
}

fn required_string(payload: &serde_json::Value, field: &str) -> Result<String, String> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("budget event is missing `{field}`"))
}

fn required_u64(payload: &serde_json::Value, field: &str) -> Result<u64, String> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("budget event is missing `{field}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parent() -> ParentExecutionBudget {
        ParentExecutionBudget::new("parent-budget", 4_000, u64::MAX, 4, 1)
    }

    fn child(parent: &ParentExecutionBudget, slot_index: usize) -> ChildExecutionBudgetReservation {
        ChildExecutionBudgetReservation {
            lease_id: format!("parent-budget:child:{slot_index}"),
            parent_budget: parent.clone(),
            parent_budget_id: parent.budget_id.clone(),
            owner_id: format!("child-{slot_index}"),
            scope: "team_agent".to_string(),
            max_tokens: 1_000,
            consumed_tokens: 0,
            deadline_at_ms: parent.deadline_at_ms,
            max_parallel: parent.max_parallel,
            revision: parent.revision,
            slot_index,
            total_slots: 4,
        }
    }

    #[test]
    fn four_children_share_one_durable_capacity_ledger() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let parent = parent();
        let ledger = ParentExecutionBudgetLedger::new(store, parent.clone()).unwrap();
        let mut reservations = (0..4)
            .map(|slot| {
                ledger
                    .reserve_provider(
                        &child(&parent, slot),
                        format!("reservation-{slot}"),
                        "capable-model",
                        10,
                        100,
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        for reservation in &mut reservations {
            reservation
                .reconcile(TokenUsage {
                    input_tokens: 10,
                    output_tokens: 20,
                    ..TokenUsage::default()
                })
                .unwrap();
        }
        let settled = ledger.snapshot().unwrap();
        assert_eq!(settled.settled_tokens, 120);
        assert!(!settled.breached);
    }

    #[test]
    fn complex_child_borrows_idle_parent_capacity_without_a_commercial_gate() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let parent = parent();
        let ledger = ParentExecutionBudgetLedger::new(store, parent.clone()).unwrap();
        let child = child(&parent, 0);
        let reservation = ledger
            .reserve_provider(
                &child,
                "elastic-child-continuation",
                "private-model-without-commercial-rate",
                10,
                child.max_tokens + 200,
            )
            .expect("capacity telemetry must never require a commercial rate");

        assert!(reservation.reserved_tokens > child.max_tokens);
    }

    #[test]
    fn crash_restart_replays_the_full_unsettled_capacity_reservation() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let parent = parent();
        let ledger = ParentExecutionBudgetLedger::new(Arc::clone(&store), parent.clone()).unwrap();
        let reservation = ledger
            .reserve_provider(
                &child(&parent, 0),
                "crash-reservation",
                "capable-model",
                10,
                100,
            )
            .unwrap();
        let before = reservation.reserved_tokens;
        drop(reservation);
        let recovered = ParentExecutionBudgetLedger::new(store, parent).unwrap();
        assert_eq!(recovered.snapshot().unwrap().reserved_tokens, before);
    }
}
