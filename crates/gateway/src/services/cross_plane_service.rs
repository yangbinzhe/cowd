use connector::{ConnectorRegistrySnapshot, ProviderAccount};
use harness_contract::execution_graph::{ExecutionGraphCommand, ExecutionNodeKind};
use runtime::ExecutionGraphHost;
use runtime::{
    ConnectorActionContext, CrossPlaneAction, CrossPlaneAuditRecord, CrossPlaneDecisionEvidence,
    CrossPlaneDispatchOutcome, CrossPlaneDispatchTarget, CrossPlaneExecutionReceipt,
    CrossPlanePolicyDecision, CrossPlaneRuntimeService,
};

use super::{CrossPlaneService, ServiceEnvelope};

struct CrossPlaneGraphResolver {
    graph_id: String,
    backend: std::sync::Arc<dyn runtime::execution_core::ScopedNodeBackend>,
}

impl runtime::execution_core::graph::executors::ScopedNodeBackendResolver
    for CrossPlaneGraphResolver
{
    fn resolve(
        &self,
        ticket: &runtime::execution_core::NodeExecutionTicket,
    ) -> Option<std::sync::Arc<dyn runtime::execution_core::ScopedNodeBackend>> {
        (ticket.graph_id == self.graph_id).then(|| std::sync::Arc::clone(&self.backend))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CrossPlaneCommitGraphError {
    #[error("{0}")]
    CanonicalActionConflict(String),
    #[error(transparent)]
    Runtime(#[from] runtime::CrossPlaneRuntimeError),
    #[error("cross-plane graph state failed: {0}")]
    State(String),
    #[error("cross-plane graph execution failed: {0}")]
    Execution(String),
}

impl CrossPlaneCommitGraphError {
    pub(crate) fn is_idempotency_conflict(&self) -> bool {
        matches!(
            self,
            Self::CanonicalActionConflict(_)
                | Self::Runtime(runtime::CrossPlaneRuntimeError::IdempotencyConflict(_))
        )
    }
}

pub(crate) struct CrossPlaneExecutionRecord {
    pub(crate) idempotency_key: Option<String>,
    pub(crate) mode: String,
    pub(crate) status: String,
    pub(crate) dispatch_status: String,
    pub(crate) action: CrossPlaneAction,
    pub(crate) decision: CrossPlanePolicyDecision,
    pub(crate) blockers: Vec<String>,
    pub(crate) dispatch_target: Option<CrossPlaneDispatchTarget>,
    pub(crate) dispatch_outcome: Option<CrossPlaneDispatchOutcome>,
    pub(crate) evidence: CrossPlaneDecisionEvidence,
    pub(crate) audit_result: String,
    pub(crate) audit_summary: String,
    pub(crate) execution_graph_id: Option<String>,
}

impl CrossPlaneService {
    pub(crate) fn preview_action(
        &self,
        idempotency_key: Option<String>,
        mode: String,
        action: CrossPlaneAction,
        decision: CrossPlanePolicyDecision,
    ) -> CrossPlaneExecutionReceipt {
        let allowed = decision.decision == runtime::PolicyDecisionKind::Allow;
        CrossPlaneExecutionReceipt::new(
            idempotency_key,
            mode,
            if allowed { "planned" } else { "blocked" },
            if allowed { "dry_run" } else { "policy_blocked" },
            action,
            decision.clone(),
            if allowed {
                Vec::new()
            } else {
                vec![format!("policy:{}", decision.reason)]
            },
            None,
        )
    }

    pub(crate) fn record_non_commit_action(
        &self,
        idempotency_key: Option<String>,
        mode: String,
        action: CrossPlaneAction,
        decision: CrossPlanePolicyDecision,
        evidence: CrossPlaneDecisionEvidence,
    ) -> Result<CrossPlaneExecutionReceipt, runtime::CrossPlaneRuntimeError> {
        let allowed = decision.decision == runtime::PolicyDecisionKind::Allow;
        let (_, receipt) = self.record_action_execution(CrossPlaneExecutionRecord {
            idempotency_key,
            mode,
            status: if allowed { "planned" } else { "blocked" }.into(),
            dispatch_status: if allowed { "dry_run" } else { "policy_blocked" }.into(),
            blockers: if allowed {
                Vec::new()
            } else {
                vec![format!("policy:{}", decision.reason)]
            },
            action,
            decision,
            dispatch_target: None,
            dispatch_outcome: None,
            evidence,
            audit_result: if allowed { "dry_run" } else { "blocked" }.into(),
            audit_summary: if allowed {
                "cross_plane_dry_run_plan"
            } else {
                "cross_plane_policy_blocked"
            }
            .into(),
            execution_graph_id: None,
        })?;
        Ok(receipt)
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: "service_ready",
            owner: self.owner,
            boundary_status: "0620_final_boundary",
        }
    }

    pub(crate) fn control(&self) -> &CrossPlaneRuntimeService {
        self.runtime_services.cross_plane()
    }

    pub(crate) fn runtime_control(&self) -> std::sync::Arc<CrossPlaneRuntimeService> {
        std::sync::Arc::clone(self.runtime_services.cross_plane())
    }

    pub(crate) fn find_execution_by_idempotency_key(
        &self,
        idempotency_key: &str,
    ) -> Option<CrossPlaneExecutionReceipt> {
        self.runtime_services
            .cross_plane()
            .find_execution_by_idempotency_key(idempotency_key)
    }

    pub(crate) fn find_execution(&self, receipt_id: &str) -> Option<CrossPlaneExecutionReceipt> {
        self.runtime_services
            .cross_plane()
            .find_execution(receipt_id)
    }

    pub(crate) fn consume_matched_grant_for_decision(
        &self,
        decision: &CrossPlanePolicyDecision,
    ) -> Option<(String, u32)> {
        self.runtime_services
            .cross_plane()
            .consume_matched_grant_for_decision(decision)
            .ok()
            .flatten()
    }

    pub(crate) fn record_action_execution(
        &self,
        record: CrossPlaneExecutionRecord,
    ) -> Result<(String, CrossPlaneExecutionReceipt), runtime::CrossPlaneRuntimeError> {
        self.record_action_execution_with_effect_commit(record, false)
    }

    pub(crate) fn record_completed_effect_execution(
        &self,
        record: CrossPlaneExecutionRecord,
    ) -> Result<(String, CrossPlaneExecutionReceipt), runtime::CrossPlaneRuntimeError> {
        self.record_action_execution_with_effect_commit(record, true)
    }

    fn record_action_execution_with_effect_commit(
        &self,
        record: CrossPlaneExecutionRecord,
        consume_single_use_grant: bool,
    ) -> Result<(String, CrossPlaneExecutionReceipt), runtime::CrossPlaneRuntimeError> {
        let audit_record = CrossPlaneAuditRecord::new(
            record.action.clone(),
            record.decision.clone(),
            record.audit_result,
            record.audit_summary,
        )
        .with_evidence(record.evidence);
        let audit_record_id = audit_record.id.clone();
        let receipt = CrossPlaneExecutionReceipt::new(
            record.idempotency_key,
            record.mode,
            record.status,
            record.dispatch_status,
            record.action,
            record.decision,
            record.blockers,
            Some(audit_record_id.clone()),
        )
        .with_dispatch_target(record.dispatch_target)
        .with_dispatch_outcome(record.dispatch_outcome)
        .with_execution_graph_id(record.execution_graph_id);
        if consume_single_use_grant {
            self.runtime_services
                .cross_plane()
                .record_completed_effect_execution(audit_record, receipt)
        } else {
            self.runtime_services
                .cross_plane()
                .record_action_execution(audit_record, receipt)
        }
    }

    pub(crate) fn decide_connector_action(
        &self,
        snapshot: &ConnectorRegistrySnapshot,
        action: CrossPlaneAction,
        mode: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> (
        CrossPlaneAction,
        CrossPlanePolicyDecision,
        CrossPlaneDecisionEvidence,
    ) {
        let connector_context = connector_context_from_snapshot(snapshot, &action, mode);
        self.runtime_services
            .cross_plane()
            .decide_with_connector_context(action, connector_context, now)
    }

    pub(crate) async fn execute_commit_graph(
        &self,
        action: &CrossPlaneAction,
        decision: &CrossPlanePolicyDecision,
        idempotency_key: &str,
        dispatch_target: Option<&CrossPlaneDispatchTarget>,
        backend: std::sync::Arc<dyn runtime::execution_core::ScopedNodeBackend>,
    ) -> Result<
        harness_contract::execution_graph::ExecutionGraphProjection,
        CrossPlaneCommitGraphError,
    > {
        let mut graph = self.runtime_services.cross_plane().compile_commit_graph(
            action,
            decision,
            idempotency_key,
        );
        let executor_kind = "cross_plane_connector".to_string();
        for node in &mut graph.nodes {
            if node.kind == ExecutionNodeKind::ToolBatch {
                node.executor_kind.clone_from(&executor_kind);
            }
        }
        let graph_id = graph.id.clone();
        match self.runtime_services.graph_state_store().load(&graph_id) {
            Ok(existing) => {
                if existing.objective != graph.objective
                    || existing.nodes != graph.nodes
                    || existing.edges != graph.edges
                {
                    return Err(CrossPlaneCommitGraphError::CanonicalActionConflict(
                        format!(
                            "cross-plane graph {graph_id} is bound to another canonical action"
                        ),
                    ));
                }
                if let Some(dispatch_target) = dispatch_target {
                    self.runtime_services
                        .cross_plane()
                        .begin_dispatch(idempotency_key, dispatch_target)?;
                }
                self.runtime_services
                    .cross_plane_connector_executor()
                    .install_resolver(std::sync::Arc::new(CrossPlaneGraphResolver {
                        graph_id: graph_id.clone(),
                        backend: std::sync::Arc::clone(&backend),
                    }));
                let projection = self
                    .runtime_services
                    .execution_supervisor()
                    .graph_projection(&graph_id)
                    .await
                    .map_err(|error| CrossPlaneCommitGraphError::Execution(error.to_string()))?;
                if projection
                    .nodes
                    .iter()
                    .all(|node| node.status.is_terminal())
                {
                    return Ok(projection);
                }
                self.runtime_services
                    .execution_supervisor()
                    .command_graph(
                        &graph_id,
                        ExecutionGraphCommand::Advance {
                            expected_revision: projection.revision,
                        },
                    )
                    .await
                    .map_err(|error| CrossPlaneCommitGraphError::Execution(error.to_string()))?;
                self.runtime_services
                    .execution_supervisor()
                    .wait_for_quiescence(&graph_id)
                    .await
                    .map_err(|error| CrossPlaneCommitGraphError::Execution(error.to_string()))?;
                return self
                    .runtime_services
                    .execution_supervisor()
                    .graph_projection(&graph_id)
                    .await
                    .map_err(|error| CrossPlaneCommitGraphError::Execution(error.to_string()));
            }
            Err(runtime::execution_core::ExecutionStateStoreError::NotFound(_)) => {}
            Err(error) => return Err(CrossPlaneCommitGraphError::State(error.to_string())),
        }
        if let Some(dispatch_target) = dispatch_target {
            self.runtime_services
                .cross_plane()
                .begin_dispatch(idempotency_key, dispatch_target)?;
        }
        self.runtime_services
            .cross_plane_connector_executor()
            .install_resolver(std::sync::Arc::new(CrossPlaneGraphResolver {
                graph_id: graph_id.clone(),
                backend,
            }));
        self.runtime_services
            .execution_supervisor()
            .submit_graph(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .map_err(|error| CrossPlaneCommitGraphError::Execution(error.to_string()))?;
        self.runtime_services
            .execution_supervisor()
            .wait_for_quiescence(&graph_id)
            .await
            .map_err(|error| CrossPlaneCommitGraphError::Execution(error.to_string()))?;
        self.runtime_services
            .execution_supervisor()
            .graph_projection(&graph_id)
            .await
            .map_err(|error| CrossPlaneCommitGraphError::Execution(error.to_string()))
    }

    /// Persist a message-delivery receipt after a caller has both executed a
    /// graph and validated its dispatch target. Generic connector graphs must
    /// not use this path: their node result is service data, not a message
    /// delivery outcome.
    pub(crate) fn record_message_dispatch_graph(
        &self,
        idempotency_key: String,
        action: CrossPlaneAction,
        decision: CrossPlanePolicyDecision,
        evidence: CrossPlaneDecisionEvidence,
        dispatch_target: CrossPlaneDispatchTarget,
        projection: &harness_contract::execution_graph::ExecutionGraphProjection,
    ) -> Result<CrossPlaneExecutionReceipt, runtime::CrossPlaneRuntimeError> {
        let outcome = projection
            .nodes
            .iter()
            .find(|node| node.kind == ExecutionNodeKind::ToolBatch)
            .and_then(|node| node.result_ref.as_deref())
            .and_then(|value| serde_json::from_str::<CrossPlaneDispatchOutcome>(value).ok());
        let (status, dispatch_status, audit_result, audit_summary, blockers) =
            graph_receipt_state(outcome.as_ref());
        let (_, receipt) = self.record_completed_effect_execution(CrossPlaneExecutionRecord {
            idempotency_key: Some(idempotency_key),
            mode: "commit".to_string(),
            status: status.to_string(),
            dispatch_status: dispatch_status.to_string(),
            action,
            decision,
            blockers,
            dispatch_target: Some(dispatch_target),
            dispatch_outcome: outcome,
            evidence,
            audit_result: audit_result.to_string(),
            audit_summary: audit_summary.to_string(),
            execution_graph_id: Some(projection.graph_id.clone()),
        })?;
        Ok(receipt)
    }
}

fn graph_receipt_state(
    outcome: Option<&CrossPlaneDispatchOutcome>,
) -> (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    Vec<String>,
) {
    match outcome.map(|outcome| outcome.status.as_str()) {
        Some("sent") => (
            "dispatched",
            "sent",
            "dispatched",
            "cross_plane_execution_graph_sent",
            Vec::new(),
        ),
        Some("delivery_uncertain") => (
            "delivery_uncertain",
            "blocked_reconciliation",
            "delivery_uncertain",
            "cross_plane_delivery_requires_reconciliation",
            vec!["delivery_uncertain:manual_reconciliation_required".into()],
        ),
        Some("failed") => (
            "blocked",
            "dispatch_failed",
            "blocked_dispatch",
            "cross_plane_execution_graph_failed",
            outcome
                .and_then(|outcome| outcome.error.clone())
                .into_iter()
                .collect(),
        ),
        _ => (
            "blocked",
            "graph_outcome_missing",
            "blocked_dispatch",
            "cross_plane_execution_graph_outcome_missing",
            vec!["execution_graph:dispatch_outcome_missing".into()],
        ),
    }
}

fn connector_context_from_snapshot(
    snapshot: &ConnectorRegistrySnapshot,
    action: &CrossPlaneAction,
    mode: &str,
) -> Option<ConnectorActionContext> {
    let capability = snapshot
        .capabilities
        .iter()
        .find(|capability| capability.capability_id == action.requested_capability)?;
    let account = connector_account_for_action(snapshot, action, &capability.provider);
    let missing_scopes = account
        .as_ref()
        .map(|account| {
            capability
                .missing_scopes(account)
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| capability.required_scopes.clone());
    Some(ConnectorActionContext {
        provider: capability.provider.clone(),
        plane: format!("{:?}", capability.plane).to_ascii_lowercase(),
        capability_id: capability.capability_id.clone(),
        provider_account: account
            .as_ref()
            .map(|account| account.account_id.clone())
            .or_else(|| action.provider_account.clone()),
        account_status: account
            .as_ref()
            .map(|account| format!("{:?}", account.health.status).to_ascii_lowercase()),
        account_reason: account
            .as_ref()
            .and_then(|account| account.health.reason.clone()),
        resource_ref: action.resource_ref.clone(),
        required_scopes: capability.required_scopes.clone(),
        missing_scopes,
        supports_commit: capability.supports_commit,
        requires_approval: capability.requires_approval,
        risk: capability.risk,
        data_classification: capability.data_classification,
        requested_mode: normalize_execute_mode(mode),
    })
}

fn connector_account_for_action<'a>(
    snapshot: &'a ConnectorRegistrySnapshot,
    action: &CrossPlaneAction,
    provider: &str,
) -> Option<&'a ProviderAccount> {
    if let Some(requested) = action
        .provider_account
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Some(account) = snapshot.accounts.iter().find(|account| {
            account.account_id == requested
                || account.provider == requested
                || account
                    .enabled_bindings
                    .iter()
                    .any(|binding| binding == requested)
        }) {
            return Some(account);
        }
    }
    snapshot.accounts.iter().find(|account| {
        account.provider == provider
            && account
                .enabled_bindings
                .iter()
                .any(|binding| binding == &action.requested_capability)
    })
}

fn normalize_execute_mode(mode: &str) -> String {
    match mode.trim().to_ascii_lowercase().as_str() {
        "commit" | "live" | "execute" => "commit".to_string(),
        _ => "dry_run".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::graph_receipt_state;
    use runtime::CrossPlaneDispatchOutcome;

    #[test]
    fn graph_receipt_status_only_reports_dispatch_for_sent_outcome() {
        let sent = CrossPlaneDispatchOutcome::sent("feishu", "send_text", "user:1", None);
        let sent_state = graph_receipt_state(Some(&sent));
        assert_eq!((sent_state.0, sent_state.1), ("dispatched", "sent"));

        let uncertain =
            CrossPlaneDispatchOutcome::delivery_uncertain("feishu", "send_text", "user:1");
        let uncertain_state = graph_receipt_state(Some(&uncertain));
        assert_eq!(
            (uncertain_state.0, uncertain_state.1),
            ("delivery_uncertain", "blocked_reconciliation")
        );

        let missing_state = graph_receipt_state(None);
        assert_eq!(
            (missing_state.0, missing_state.1),
            ("blocked", "graph_outcome_missing")
        );
    }
}
