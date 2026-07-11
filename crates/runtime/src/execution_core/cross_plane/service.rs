use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::cross_plane_policy::CrossPlaneControlSnapshot;
use crate::runtime_event_store::RuntimeEventStoreError;
use crate::runtime_event_store::RuntimeTransactionEventInput;
use crate::{
    ConnectorActionContext, CrossPlaneAction, CrossPlaneAuditRecord, CrossPlaneControlPlane,
    CrossPlaneDecisionEvidence, CrossPlaneDispatchOutcome, CrossPlaneDispatchTarget,
    CrossPlaneExecutionReceipt, CrossPlaneGrant, CrossPlaneIdentityBinding,
    CrossPlanePolicyDecision, CrossPlaneResolvedIdentity, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore,
};

const STREAM_ID: &str = "cross-plane:control";

#[derive(Debug, Error)]
pub enum CrossPlaneRuntimeError {
    #[error(transparent)]
    Store(#[from] RuntimeEventStoreError),
    #[error("cross-plane state serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("cross-plane state query failed: {0}")]
    Query(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CrossPlaneDispatchRecovery {
    NotStarted,
    DeliveryUncertain {
        resolution: String,
        target: CrossPlaneDispatchTarget,
    },
    Reconciled {
        outcome: CrossPlaneDispatchOutcome,
    },
}

pub struct CrossPlaneRuntimeService {
    control: CrossPlaneControlPlane,
    event_store: Arc<RuntimeEventStore>,
    mutation_lock: Mutex<()>,
}

impl CrossPlaneRuntimeService {
    pub fn open(event_store: Arc<RuntimeEventStore>) -> Result<Self, CrossPlaneRuntimeError> {
        let control = CrossPlaneControlPlane::new();
        if let Some(event) = event_store
            .latest_for_stream(STREAM_ID)
            .map_err(CrossPlaneRuntimeError::Query)?
        {
            control.replace_snapshot(serde_json::from_value(event.payload)?);
        }
        Ok(Self {
            control,
            event_store,
            mutation_lock: Mutex::new(()),
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> CrossPlaneControlSnapshot {
        self.control.snapshot()
    }
    #[must_use]
    pub fn summary(&self, now: DateTime<Utc>) -> crate::CrossPlaneSummary {
        self.control.summary(now)
    }
    #[must_use]
    pub fn list_grants(&self) -> Vec<CrossPlaneGrant> {
        self.control.list_grants()
    }
    #[must_use]
    pub fn list_identities(&self) -> Vec<CrossPlaneIdentityBinding> {
        self.control.list_identities()
    }
    #[must_use]
    pub fn list_audit(&self, limit: usize, offset: usize) -> Vec<CrossPlaneAuditRecord> {
        self.control.list_audit(limit, offset)
    }
    #[must_use]
    pub fn list_executions(&self, limit: usize, offset: usize) -> Vec<CrossPlaneExecutionReceipt> {
        self.control.list_executions(limit, offset)
    }
    #[must_use]
    pub fn find_execution_by_idempotency_key(
        &self,
        key: &str,
    ) -> Option<CrossPlaneExecutionReceipt> {
        self.control.find_execution_by_idempotency_key(key)
    }
    #[must_use]
    pub fn resolve_identity(
        &self,
        identity_ref: &str,
        now: DateTime<Utc>,
    ) -> Option<CrossPlaneResolvedIdentity> {
        self.control.resolve_identity(identity_ref, now)
    }

    pub fn upsert_identity(
        &self,
        value: CrossPlaneIdentityBinding,
    ) -> Result<CrossPlaneIdentityBinding, CrossPlaneRuntimeError> {
        self.mutate("identity_upserted", |control| {
            control.upsert_identity(value)
        })
    }
    pub fn revoke_identity(&self, id: &str) -> Result<bool, CrossPlaneRuntimeError> {
        self.mutate("identity_revoked", |control| control.revoke_identity(id))
    }
    pub fn upsert_grant(
        &self,
        value: CrossPlaneGrant,
    ) -> Result<CrossPlaneGrant, CrossPlaneRuntimeError> {
        self.mutate("grant_upserted", |control| control.upsert_grant(value))
    }
    pub fn revoke_grant(&self, id: &str) -> Result<bool, CrossPlaneRuntimeError> {
        self.mutate("grant_revoked", |control| control.revoke_grant(id))
    }
    pub fn consume_matched_grant_for_decision(
        &self,
        decision: &CrossPlanePolicyDecision,
    ) -> Result<Option<(String, u32)>, CrossPlaneRuntimeError> {
        self.mutate("grant_consumed", |control| {
            control.consume_matched_grant_for_decision(decision)
        })
    }

    #[must_use]
    pub fn decide_with_connector_context(
        &self,
        action: CrossPlaneAction,
        context: Option<ConnectorActionContext>,
        now: DateTime<Utc>,
    ) -> (
        CrossPlaneAction,
        CrossPlanePolicyDecision,
        CrossPlaneDecisionEvidence,
    ) {
        self.control
            .decide_with_connector_context(action, context, now)
    }

    pub fn record_action_execution(
        &self,
        audit: CrossPlaneAuditRecord,
        receipt: CrossPlaneExecutionReceipt,
    ) -> Result<(String, CrossPlaneExecutionReceipt), CrossPlaneRuntimeError> {
        self.mutate("execution_recorded", move |control| {
            if let Some(existing) = receipt
                .idempotency_key
                .as_deref()
                .and_then(|key| control.find_execution_by_idempotency_key(key))
            {
                return (
                    existing.audit_record_id.clone().unwrap_or_default(),
                    existing,
                );
            }
            let audit_id = audit.id.clone();
            control.record_audit(audit);
            control.record_execution(receipt.clone());
            (audit_id, receipt)
        })
    }

    pub fn begin_dispatch(
        &self,
        idempotency_key: &str,
        target: &CrossPlaneDispatchTarget,
    ) -> Result<(), CrossPlaneRuntimeError> {
        let _guard = self
            .mutation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let stream_id = dispatch_stream_id(idempotency_key);
        if self
            .event_store
            .event_by_idempotency_key(&stream_id, "dispatch-intent")?
            .is_some()
        {
            return Ok(());
        }
        let revision = self.event_store.stream_revision(&stream_id)?;
        self.event_store.append_batch_if_revision(
            stream_id.clone(),
            revision,
            format!(
                "cross-plane-dispatch-intent:{}",
                dispatch_key_digest(idempotency_key)
            ),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::CrossPlane,
                    kind: "cross_plane.dispatch_intent".to_string(),
                    status: Some("pending".to_string()),
                    actor: Some("cross_plane_runtime".to_string()),
                    refs: vec![RuntimeEventRef {
                        kind: "idempotency_key".to_string(),
                        id: idempotency_key.to_string(),
                    }],
                    payload: serde_json::json!({
                        "idempotency_key": idempotency_key,
                        "target": target,
                    }),
                },
                idempotency_key: Some("dispatch-intent".to_string()),
                schema_version: 1,
            }],
        )?;
        Ok(())
    }

    pub fn complete_dispatch(
        &self,
        idempotency_key: &str,
        outcome: &CrossPlaneDispatchOutcome,
    ) -> Result<CrossPlaneDispatchOutcome, CrossPlaneRuntimeError> {
        let _guard = self
            .mutation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = self.dispatch_receipt(idempotency_key)? {
            return Ok(existing);
        }
        let stream_id = dispatch_stream_id(idempotency_key);
        let revision = self.event_store.stream_revision(&stream_id)?;
        self.event_store.append_batch_if_revision(
            stream_id.clone(),
            revision,
            format!(
                "cross-plane-dispatch-receipt:{}",
                dispatch_key_digest(idempotency_key)
            ),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::CrossPlane,
                    kind: "cross_plane.dispatch_receipt".to_string(),
                    status: Some(outcome.status.clone()),
                    actor: Some("cross_plane_runtime".to_string()),
                    refs: vec![RuntimeEventRef {
                        kind: "idempotency_key".to_string(),
                        id: idempotency_key.to_string(),
                    }],
                    payload: serde_json::to_value(outcome)?,
                },
                idempotency_key: Some("dispatch-receipt".to_string()),
                schema_version: 1,
            }],
        )?;
        Ok(outcome.clone())
    }

    pub fn reconcile_dispatch(
        &self,
        idempotency_key: &str,
        outcome: &CrossPlaneDispatchOutcome,
    ) -> Result<CrossPlaneDispatchOutcome, CrossPlaneRuntimeError> {
        if self.dispatch_intent(idempotency_key)?.is_none() {
            return Err(CrossPlaneRuntimeError::Query(format!(
                "dispatch `{idempotency_key}` has no durable intent"
            )));
        }
        let outcome = self.complete_dispatch(idempotency_key, outcome)?;
        self.mutate("dispatch_reconciled", |control| {
            control.reconcile_execution(idempotency_key, outcome.clone())
        })?;
        Ok(outcome)
    }

    pub fn dispatch_receipt(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<CrossPlaneDispatchOutcome>, CrossPlaneRuntimeError> {
        self.event_store
            .event_by_idempotency_key(&dispatch_stream_id(idempotency_key), "dispatch-receipt")?
            .map(|event| serde_json::from_value(event.payload).map_err(Into::into))
            .transpose()
    }

    pub fn dispatch_intent(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<CrossPlaneDispatchTarget>, CrossPlaneRuntimeError> {
        self.event_store
            .event_by_idempotency_key(&dispatch_stream_id(idempotency_key), "dispatch-intent")?
            .map(|event| {
                event
                    .payload
                    .get("target")
                    .cloned()
                    .ok_or_else(|| {
                        CrossPlaneRuntimeError::Query("dispatch intent has no target".into())
                    })
                    .and_then(|target| serde_json::from_value(target).map_err(Into::into))
            })
            .transpose()
    }

    pub fn dispatch_recovery(
        &self,
        idempotency_key: &str,
    ) -> Result<CrossPlaneDispatchRecovery, CrossPlaneRuntimeError> {
        if let Some(outcome) = self.dispatch_receipt(idempotency_key)? {
            return Ok(CrossPlaneDispatchRecovery::Reconciled { outcome });
        }
        Ok(match self.dispatch_intent(idempotency_key)? {
            Some(target) => CrossPlaneDispatchRecovery::DeliveryUncertain {
                resolution: "blocked_reconciliation".into(),
                target,
            },
            None => CrossPlaneDispatchRecovery::NotStarted,
        })
    }

    #[must_use]
    pub fn compile_commit_graph(
        &self,
        action: &CrossPlaneAction,
        decision: &CrossPlanePolicyDecision,
        idempotency_key: &str,
    ) -> ExecutionGraph {
        let mut graph = ExecutionGraph::new(format!(
            "cross-plane commit: {}",
            action.requested_capability
        ));
        let digest = Sha256::digest(idempotency_key.as_bytes());
        graph.id = format!("cross-plane-graph-{digest:x}");
        let mut tool = ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "cross_plane_connector",
            serde_json::to_string(action).unwrap_or_default(),
        );
        tool.idempotency_key = format!("{idempotency_key}:tool");
        let tool_id = tool.id.clone();
        if let Some(required_approval) = decision.required_approval.as_ref() {
            let approval_summary =
                format!("Cross-plane action requires {required_approval:?} approval");
            let mut approval = ExecutionNodeSpec::new(
                ExecutionNodeKind::Approval,
                "approval",
                serde_json::json!({
                    "action": action.requested_capability,
                    "summary": approval_summary,
                    "evidence_refs": [],
                })
                .to_string(),
            );
            approval.idempotency_key = format!("{idempotency_key}:approval");
            let approval_id = approval.id.clone();
            graph.nodes.push(approval);
            graph.edges.push(ExecutionEdge {
                from: approval_id,
                to: tool_id,
                kind: ExecutionEdgeKind::DependsOn,
            });
        }
        graph.nodes.push(tool);
        graph
    }

    fn mutate<T>(
        &self,
        kind: &str,
        operation: impl FnOnce(&CrossPlaneControlPlane) -> T,
    ) -> Result<T, CrossPlaneRuntimeError> {
        let _guard = self
            .mutation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = self.control.snapshot();
        let result = operation(&self.control);
        let revision = self.event_store.stream_revision(STREAM_ID)?;
        let payload = serde_json::to_value(self.control.snapshot())?;
        let append = self.event_store.append_batch_if_revision(
            STREAM_ID,
            revision,
            format!("cross-plane:{kind}:{}", uuid::Uuid::new_v4()),
            vec![RuntimeEventInput {
                stream_id: STREAM_ID.to_string(),
                scope: RuntimeEventScope::CrossPlane,
                kind: kind.to_string(),
                status: Some("committed".to_string()),
                actor: Some("cross_plane_runtime".to_string()),
                refs: vec![RuntimeEventRef {
                    kind: "workspace".to_string(),
                    id: STREAM_ID.to_string(),
                }],
                payload,
            }
            .into()],
        );
        if let Err(error) = append {
            self.control.replace_snapshot(before);
            return Err(error.into());
        }
        Ok(result)
    }
}

fn dispatch_key_digest(idempotency_key: &str) -> String {
    format!("{:x}", Sha256::digest(idempotency_key.as_bytes()))
}

fn dispatch_stream_id(idempotency_key: &str) -> String {
    format!(
        "cross-plane:dispatch:{}",
        dispatch_key_digest(idempotency_key)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn workspace_state_is_isolated_and_durable() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = CrossPlaneRuntimeService::open(Arc::clone(&store)).unwrap();
        service
            .upsert_grant(CrossPlaneGrant::persistent("alice", "channel.send"))
            .unwrap();
        assert_eq!(
            CrossPlaneRuntimeService::open(store)
                .unwrap()
                .list_grants()
                .len(),
            1
        );
        let isolated = CrossPlaneRuntimeService::open(Arc::new(
            RuntimeEventStore::try_open_in_memory().unwrap(),
        ))
        .unwrap();
        assert!(isolated.list_grants().is_empty());
    }
    #[test]
    fn commit_compiles_to_execution_graph() {
        let service = CrossPlaneRuntimeService::open(Arc::new(
            RuntimeEventStore::try_open_in_memory().unwrap(),
        ))
        .unwrap();
        let action = CrossPlaneAction::new("alice", "channel.send");
        let (_, decision, _) =
            service.decide_with_connector_context(action.clone(), None, Utc::now());
        let graph = service.compile_commit_graph(&action, &decision, "request-1");
        assert!(graph
            .nodes
            .iter()
            .any(|node| node.kind == ExecutionNodeKind::ToolBatch));
    }

    #[test]
    fn execution_receipt_is_idempotent_without_duplicate_audit() {
        let service = CrossPlaneRuntimeService::open(Arc::new(
            RuntimeEventStore::try_open_in_memory().unwrap(),
        ))
        .unwrap();
        let action = CrossPlaneAction::new("alice", "channel.send");
        let (_, decision, evidence) =
            service.decide_with_connector_context(action.clone(), None, Utc::now());
        let build = || {
            let audit = CrossPlaneAuditRecord::new(
                action.clone(),
                decision.clone(),
                "planned",
                "graph queued",
            )
            .with_evidence(evidence.clone());
            let receipt = CrossPlaneExecutionReceipt::new(
                Some("stable-key".to_string()),
                "commit",
                "planned",
                "execution_graph_queued",
                action.clone(),
                decision.clone(),
                Vec::new(),
                Some(audit.id.clone()),
            );
            (audit, receipt)
        };
        let (audit, receipt) = build();
        let first = service.record_action_execution(audit, receipt).unwrap();
        let (audit, receipt) = build();
        let second = service.record_action_execution(audit, receipt).unwrap();
        assert_eq!(first.1.id, second.1.id);
        assert_eq!(service.list_audit(10, 0).len(), 1);
        assert_eq!(service.list_executions(10, 0).len(), 1);
    }

    #[test]
    fn dispatch_intent_and_receipt_survive_crash_window_and_restart() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = CrossPlaneRuntimeService::open(Arc::clone(&store)).unwrap();
        let target = CrossPlaneDispatchTarget {
            platform: Some("lark".to_string()),
            ready: true,
            ..CrossPlaneDispatchTarget::default()
        };
        service.begin_dispatch("stable-send", &target).unwrap();
        assert_eq!(
            service.dispatch_intent("stable-send").unwrap(),
            Some(target.clone())
        );
        assert!(service.dispatch_receipt("stable-send").unwrap().is_none());
        assert_eq!(
            service.dispatch_recovery("stable-send").unwrap(),
            CrossPlaneDispatchRecovery::DeliveryUncertain {
                resolution: "blocked_reconciliation".into(),
                target: target.clone(),
            }
        );

        // Simulate restart after the backend accepted the idempotency key but
        // before the runtime persisted its receipt.
        let restarted = CrossPlaneRuntimeService::open(Arc::clone(&store)).unwrap();
        restarted.begin_dispatch("stable-send", &target).unwrap();
        let action = CrossPlaneAction::new("alice", "channel.send");
        let (_, decision, _) =
            restarted.decide_with_connector_context(action.clone(), None, Utc::now());
        let audit = CrossPlaneAuditRecord::new(
            action.clone(),
            decision.clone(),
            "delivery_uncertain",
            "manual reconciliation required",
        );
        restarted
            .record_action_execution(
                audit.clone(),
                CrossPlaneExecutionReceipt::new(
                    Some("stable-send".into()),
                    "commit",
                    "delivery_uncertain",
                    "blocked_reconciliation",
                    action,
                    decision,
                    vec!["delivery_uncertain:manual_reconciliation_required".into()],
                    Some(audit.id),
                ),
            )
            .unwrap();
        let outcome = CrossPlaneDispatchOutcome::sent(
            "lark",
            "send_text",
            "lark:user",
            Some("provider-message-1".to_string()),
        );
        restarted
            .reconcile_dispatch("stable-send", &outcome)
            .unwrap();
        let reconciled = restarted
            .find_execution_by_idempotency_key("stable-send")
            .unwrap();
        assert_eq!(reconciled.status, "dispatched");
        assert_eq!(reconciled.dispatch_status, "sent");
        let replay = CrossPlaneRuntimeService::open(store).unwrap();
        assert_eq!(
            replay.dispatch_receipt("stable-send").unwrap(),
            Some(outcome)
        );
    }
}
