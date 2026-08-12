use std::sync::Arc;

use harness_contract::execution_graph::{ExecutionGraphCommand, ExecutionNodeStatus};
use harness_contract::policy::{PolicyDecisionKind, RiskAssessment, RiskGateReceipt, RiskLevel};
use runtime::{ApprovalConfig, ExecutionGraphHost};

use super::ServiceEnvelope;

#[derive(Debug, Clone, Default)]
pub(crate) struct ApprovalPendingFilter {
    pub(crate) session_id: Option<String>,
    pub(crate) domain: Option<harness_contract::policy::ApprovalDomain>,
    pub(crate) blocks_execution: Option<bool>,
}

#[derive(Clone)]
pub(crate) struct ApprovalService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    runtime: Option<Arc<crate::runtime_service::RuntimeService>>,
    runtime_services: Option<Arc<runtime::RuntimeServices>>,
}

impl ApprovalService {
    pub(crate) fn new() -> Self {
        Self {
            label: "approval",
            owner: "0.9.296 Approval service boundary",
            runtime: None,
            runtime_services: None,
        }
    }

    pub(crate) fn with_runtime_services(
        mut self,
        runtime_services: Arc<runtime::RuntimeServices>,
    ) -> Self {
        self.runtime_services = Some(Arc::clone(&runtime_services));
        self
    }

    pub(crate) fn with_runtime(
        mut self,
        runtime: Arc<crate::runtime_service::RuntimeService>,
    ) -> Self {
        self.runtime = Some(runtime);
        self
    }

    fn runtime_services(&self) -> Result<&runtime::RuntimeServices, String> {
        self.runtime_services
            .as_deref()
            .ok_or_else(|| "runtime services are not configured".to_string())
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: if self.runtime_services.is_some() {
                "service_ready"
            } else {
                "service_boundary_ready"
            },
            owner: self.owner,
            boundary_status: "0620_final_boundary",
        }
    }

    pub(crate) fn is_configured(&self) -> bool {
        self.runtime_services.is_some()
    }

    pub(crate) async fn pending(
        &self,
        principal: &runtime::VerifiedPrincipal,
    ) -> serde_json::Value {
        self.pending_filtered(principal, ApprovalPendingFilter::default())
            .await
    }

    /// Deny pending approvals that have been waiting longer than
    /// `older_than_days` (P3). Every denial is an audited decision; nothing
    /// is hard-deleted. Returns per-item success/failure lists.
    pub(crate) async fn prune(
        &self,
        older_than_days: u64,
        reason: Option<String>,
        principal: &runtime::VerifiedPrincipal,
    ) -> Result<serde_json::Value, String> {
        if older_than_days == 0 || older_than_days > 365 {
            return Err("approval_prune_days_out_of_range".to_string());
        }
        if !principal.is_human_interactive() || !principal.has_capability("approval.respond") {
            return Err("approval_human_interactive_capability_required".to_string());
        }
        let Some(services) = self.runtime_services.as_deref() else {
            return Err("approval_service_not_ready".to_string());
        };
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let cutoff = now_ms.saturating_sub(older_than_days.saturating_mul(86_400_000));
        let ids = services
            .approval_queue()
            .list()
            .into_iter()
            .filter(|request| approval_visible_to(request, principal))
            .filter(|request| request.created_at_ms < cutoff)
            .map(|request| request.approval_id.clone())
            .collect::<Vec<_>>();
        let mut pruned = Vec::new();
        let mut failed = Vec::new();
        for id in &ids {
            let decision_reason = reason.clone().unwrap_or_else(|| {
                format!("pruned after {older_than_days} days without a decision")
            });
            match self
                .respond(
                    id,
                    false,
                    false,
                    runtime::ApprovalGrantScope::Once,
                    Some(decision_reason),
                    principal,
                )
                .await
            {
                Ok(_) => pruned.push(id.clone()),
                Err(error) => failed.push(format!("{id}: {error}")),
            }
        }
        Ok(serde_json::json!({
            "pruned": pruned.len(),
            "failed": failed.len(),
            "older_than_days": older_than_days,
            "approval_ids": pruned,
            "failures": failed,
        }))
    }

    pub(crate) async fn pending_filtered(
        &self,
        principal: &runtime::VerifiedPrincipal,
        filter: ApprovalPendingFilter,
    ) -> serde_json::Value {
        if let Some(services) = self.runtime_services.as_deref() {
            services.approval_queue().refresh();
        }
        let (projection, pending) = self.runtime_services.as_deref().map_or_else(
            || (serde_json::Value::Null, Vec::new()),
            |services| {
                let requests = services
                    .approval_queue()
                    .list()
                    .into_iter()
                    .filter(|request| approval_visible_to(request, principal))
                    .filter(|request| {
                        filter.session_id.as_ref().is_none_or(|session_id| {
                            request.context.session_id.as_deref() == Some(session_id.as_str())
                        })
                    })
                    .filter(|request| filter.domain.is_none_or(|domain| request.domain == domain))
                    .filter(|request| {
                        filter
                            .blocks_execution
                            .is_none_or(|blocks| request.blocks_execution == blocks)
                    })
                    .collect::<Vec<_>>();
                let pending = requests
                    .iter()
                    .filter(|request| request.status == runtime::GlobalApprovalStatus::Pending)
                    .cloned()
                    .collect::<Vec<_>>();
                (
                    serde_json::json!({
                        "kind": "runtime.global_approvals",
                        "count": requests.len(),
                        "pending_count": pending.len(),
                        "requests": requests,
                    }),
                    pending,
                )
            },
        );
        serde_json::json!({
            "kind": "gateway.unified_approval_pending",
            "filter": {
                "session_id": filter.session_id,
                "domain": filter.domain.map(harness_contract::policy::ApprovalDomain::as_str),
                "blocks_execution": filter.blocks_execution,
            },
            "pending": pending,
            "approvals": projection,
        })
    }

    pub(crate) async fn config(&self) -> ApprovalConfig {
        match &self.runtime_services {
            Some(services) => services.approval_coordinator().config().await,
            None => ApprovalConfig::default(),
        }
    }

    pub(crate) async fn update_config(&self, config: ApprovalConfig) -> ApprovalConfig {
        if let Some(services) = &self.runtime_services {
            services
                .approval_coordinator()
                .update_config(config.clone())
                .await;
        }
        config
    }

    pub(crate) async fn history(
        &self,
        limit: usize,
        offset: usize,
        principal: &runtime::VerifiedPrincipal,
    ) -> serde_json::Value {
        let mut combined = Vec::new();
        if let Some(services) = self.runtime_services.as_deref() {
            services.approval_queue().refresh();
            combined.extend(
                services
                    .approval_queue()
                    .list()
                    .into_iter()
                    .filter(|request| request.status != runtime::GlobalApprovalStatus::Pending)
                    .filter(|request| approval_visible_to(request, principal))
                    .filter_map(|request| serde_json::to_value(request).ok()),
            );
        }
        combined.sort_by(|left, right| {
            let left_time = approval_history_timestamp(left);
            let right_time = approval_history_timestamp(right);
            right_time.cmp(&left_time)
        });
        serde_json::Value::Array(combined.into_iter().skip(offset).take(limit).collect())
    }

    /// Resolve one approval by canonical id without imposing the UI history
    /// page size.  Backlinks are durable identities, not a request to inspect
    /// only the most recent 200 records.
    pub(crate) async fn exact(
        &self,
        id: &str,
        principal: &runtime::VerifiedPrincipal,
    ) -> Option<serde_json::Value> {
        if let Some(services) = self.runtime_services.as_deref() {
            services.approval_queue().refresh();
            if let Some(request) = services
                .approval_queue()
                .list()
                .into_iter()
                .filter(|request| approval_visible_to(request, principal))
                .find(|request| request.approval_id == id)
            {
                return serde_json::to_value(request).ok();
            }
        }
        None
    }

    pub(crate) async fn grants(
        &self,
        principal: &runtime::VerifiedPrincipal,
    ) -> Result<serde_json::Value, String> {
        if !principal.is_human_interactive() || !principal.has_capability("approval.respond") {
            return Err("approval_human_interactive_capability_required".to_string());
        }
        let services = self.runtime_services()?;
        services.approval_queue().refresh();
        let grants = services.approval_queue().grants();
        let active_count = grants
            .iter()
            .filter(|grant| grant.status == harness_contract::policy::ApprovalGrantStatus::Active)
            .count();
        Ok(serde_json::json!({
            "kind": "runtime.approval_grants",
            "count": grants.len(),
            "active_count": active_count,
            "grants": grants,
        }))
    }

    pub(crate) async fn revoke_grant(
        &self,
        grant_id: &str,
        reason: &str,
        principal: &runtime::VerifiedPrincipal,
    ) -> Result<serde_json::Value, String> {
        let services = self.runtime_services()?;
        services.approval_queue().refresh();
        let grant = services
            .approval_queue()
            .revoke_grant(principal, grant_id, reason)?;
        Ok(serde_json::json!({
            "kind": "runtime.approval_grant_revoked",
            "grant": grant,
            "grants": services.approval_queue().projection(),
        }))
    }

    pub(crate) async fn respond(
        &self,
        id: &str,
        approved: bool,
        skip: bool,
        scope: runtime::approval_queue::ApprovalGrantScope,
        reason: Option<String>,
        principal: &runtime::VerifiedPrincipal,
    ) -> Result<serde_json::Value, String> {
        if skip && approved {
            return Err("approval_skip_and_approve_are_mutually_exclusive".to_string());
        }
        if !principal.is_human_interactive() || !principal.has_capability("approval.respond") {
            return Err("approval_human_interactive_capability_required".to_string());
        }
        let services = self.runtime_services()?;
        if services
            .approval_queue()
            .get(id)
            .is_some_and(|request| request.source.typed_application().is_some())
        {
            return Err("application_review_requires_typed_decision_service".to_string());
        }
        let graph_target = canonical_graph_approval_target(id);
        let graph_before = if let Some((graph_id, node_id)) = &graph_target {
            let graph = services
                .graph_state_store()
                .load_async(graph_id.clone())
                .await
                .map_err(|error| format!("approval_graph_not_found: {error}"))?;
            match graph.node_statuses.get(node_id) {
                Some(ExecutionNodeStatus::WaitingApproval) => {}
                Some(ExecutionNodeStatus::Completed) if approved => {
                    return Ok(serde_json::json!({
                        "id": id,
                        "resolved": true,
                        "approved": true,
                        "status": "already_applied",
                        "graph_id": graph_id,
                        "node_id": node_id,
                        "graph_revision": graph.revision,
                    }));
                }
                Some(ExecutionNodeStatus::Cancelled) if !approved => {
                    return Ok(serde_json::json!({
                        "id": id,
                        "resolved": true,
                        "approved": false,
                        "status": "already_applied",
                        "graph_id": graph_id,
                        "node_id": node_id,
                        "graph_revision": graph.revision,
                    }));
                }
                Some(status) => {
                    return Err(format!(
                        "approval_invalid_state: graph `{graph_id}` node `{node_id}` is {status:?}"
                    ));
                }
                None => {
                    return Err(format!(
                        "approval_node_not_found: graph `{graph_id}` has no node `{node_id}`"
                    ));
                }
            }
            Some(graph)
        } else {
            None
        };
        let decision_reason = reason.unwrap_or_else(|| {
            if skip {
                "skipped by user; execution may continue on read-only/reversible nodes".to_string()
            } else if approved {
                "approved via gateway approval API".to_string()
            } else {
                "denied via gateway approval API".to_string()
            }
        });
        let graph_receipt = if !skip {
            if let (Some((graph_id, node_id)), Some(graph)) = (graph_target, graph_before) {
                let command = ExecutionGraphCommand::SubmitApproval {
                    expected_revision: graph.revision,
                    node_id: node_id.clone(),
                    approved,
                    decision_ref: format!("approval-decision:{id}"),
                };
                let graph_receipt = services
                    .execution_supervisor()
                    .command_graph(&graph_id, command)
                    .await
                    .map_err(|error| format!("approval_graph_command_failed: {error}"))?;
                let graph = services
                    .execution_supervisor()
                    .graph_projection(&graph_id)
                    .await
                    .map_err(|error| format!("approval_graph_projection_failed: {error}"))?;
                services.approval_queue().refresh();
                Some(serde_json::json!({
                    "graph_id": graph_id,
                    "node_id": node_id,
                    "revision": graph_receipt.accepted_revision,
                    "node_status": graph.nodes.iter()
                        .find(|node| node.node_id == node_id)
                        .map(|node| node.status),
                }))
            } else {
                None
            }
        } else {
            None
        };
        let receipt = if graph_receipt.is_some() {
            services.approval_queue().refresh();
            serde_json::to_value(
                services
                    .approval_queue()
                    .get(id)
                    .ok_or_else(|| format!("approval_projection_missing: {id}"))?,
            )
            .map_err(|error| error.to_string())?
        } else {
            serde_json::to_value(services.approval_queue().decide(
                principal,
                runtime::ApprovalDecisionCommand {
                    approval_id: id.to_string(),
                    approved,
                    skip,
                    reason: decision_reason,
                    scope,
                    actor: harness_contract::policy::ApprovalDecisionActor {
                        kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                        actor_id: principal.claims().principal_id.clone(),
                    },
                    evidence_refs: vec!["gateway.approval.respond".to_string()],
                },
            )?)
            .map_err(|error| error.to_string())?
        };
        services.approval_coordinator().notify_decision(id);
        if let Some(request) = services.approval_queue().get(id) {
            self.emit_approval_resolved(&request);
        }
        Ok(serde_json::json!({
            "id": id,
            "resolved": true,
            "approved": approved,
            "skipped": skip,
            "receipt": receipt,
            "execution_graph": graph_receipt,
            "approvals": services.approval_queue().projection(),
        }))
    }

    fn emit_approval_resolved(&self, request: &runtime::GlobalApprovalRequest) {
        let Some(session_id) = request.source.session_id.as_deref() else {
            return;
        };
        let Some(runtime_service) = self.runtime.as_deref() else {
            return;
        };
        let _ = runtime_service.emit_session_event(
            session_id,
            runtime::CowdEvent::ApprovalResolved {
                request_id: request.approval_id.clone(),
                status: request.status,
                scope: request.decision.as_ref().map(|decision| decision.scope),
                actor_id: request
                    .decision
                    .as_ref()
                    .map(|decision| decision.actor.actor_id.clone()),
            },
        );
    }

    pub(crate) async fn risk_receipt(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Result<RiskGateReceipt, String> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| "runtime tool catalog is not configured".to_string())?;
        let input = serde_json::from_str(input)
            .unwrap_or_else(|_| serde_json::Value::String(input.to_string()));
        let descriptor = runtime
            .registered_tool_effect(tool_name, &input)
            .ok_or_else(|| format!("registered tool effect is unavailable: {tool_name}"))?;
        let risk = runtime::task_risk_for_effect(&descriptor);
        let level = match risk {
            harness_contract::core::TaskRisk::Low => RiskLevel::Low,
            harness_contract::core::TaskRisk::Medium => RiskLevel::Medium,
            harness_contract::core::TaskRisk::High => RiskLevel::High,
            harness_contract::core::TaskRisk::Critical => RiskLevel::Critical,
        };
        let scope = descriptor.scopes.first().cloned().unwrap_or(
            harness_contract::policy::PermissionScope {
                resource: harness_contract::policy::PermissionResource::Tool,
                operation: harness_contract::policy::PermissionOperation::Execute,
                target: Some(tool_name.to_string()),
            },
        );
        let approval_required = !matches!(risk, harness_contract::core::TaskRisk::Low);
        Ok(RiskGateReceipt {
            scope,
            risk: RiskAssessment {
                level,
                reasons: vec![
                    format!("descriptor:{}", descriptor.descriptor_hash),
                    format!("effect:{:?}", descriptor.effect_kind).to_ascii_lowercase(),
                ],
                assessed_at: chrono::Utc::now(),
            },
            decision: if approval_required {
                PolicyDecisionKind::Ask
            } else {
                PolicyDecisionKind::Allow
            },
            approval_required,
            issued_at: chrono::Utc::now(),
        })
    }
}

fn approval_history_timestamp(value: &serde_json::Value) -> i64 {
    value
        .get("resolved_at_ms")
        .and_then(serde_json::Value::as_u64)
        .or_else(|| {
            value
                .get("timestamp_ms")
                .and_then(serde_json::Value::as_u64)
        })
        .map_or_else(
            || {
                value
                    .get("resolved_at")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
                    .map_or(0, |timestamp| timestamp.timestamp_millis())
            },
            |timestamp| timestamp.min(i64::MAX as u64) as i64,
        )
}

fn approval_visible_to(
    request: &runtime::GlobalApprovalRequest,
    principal: &runtime::VerifiedPrincipal,
) -> bool {
    let Some(application) = request.source.typed_application() else {
        return true;
    };
    principal.is_human_interactive()
        && principal.has_capability("approval.respond")
        && principal.has_capability(&application.decision_capability)
        && request
            .source
            .resource_ref
            .as_deref()
            .is_some_and(|resource| {
                principal.claims().scopes.iter().any(|scope| {
                    scope == "gateway"
                        || scope == resource
                        || resource.starts_with(&format!("{scope}:"))
                })
            })
}

fn canonical_graph_approval_target(approval_id: &str) -> Option<(String, String)> {
    runtime::execution_core::graph::executors::parse_graph_approval_id(approval_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
    };
    use runtime::ExecutionGraphHost;

    #[tokio::test]
    async fn approval_decision_commits_graph_and_approval_stream_together() {
        let services = runtime::RuntimeServices::in_memory().unwrap();
        let mut graph =
            ExecutionGraph::new("approval reconciliation").with_lineage(ExecutionGraphLineage {
                session_id: "approval-service-session".to_string(),
                turn_id: "approval-service-turn".to_string(),
                root_task_id: "approval-service-task".to_string(),
                task_id: "approval-service-task".to_string(),
                generation: 1,
            });
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Approval,
            "approval",
            serde_json::json!({
                "action": "channel.send",
                "summary": "approve dispatch",
            })
            .to_string(),
        );
        let node_id = node.id.clone();
        graph.nodes.push(node);
        let graph_id = graph.id.clone();
        services
            .execution_supervisor()
            .submit_graph(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .unwrap();
        services
            .execution_supervisor()
            .wait_for_quiescence(&graph_id)
            .await
            .unwrap();
        let waiting = services
            .execution_supervisor()
            .projection(&graph_id)
            .await
            .unwrap();
        assert_eq!(
            waiting
                .nodes
                .iter()
                .find(|node| node.node_id == node_id)
                .map(|node| node.status),
            Some(ExecutionNodeStatus::WaitingApproval)
        );
        let approval_id =
            runtime::execution_core::graph::executors::graph_approval_id(&graph_id, &node_id);
        services
            .execution_supervisor()
            .command_graph(
                &graph_id,
                ExecutionGraphCommand::SubmitApproval {
                    expected_revision: waiting.revision,
                    node_id: node_id.clone(),
                    approved: true,
                    decision_ref: format!("approval-decision:{approval_id}"),
                },
            )
            .await
            .unwrap();
        services
            .execution_supervisor()
            .wait_for_quiescence(&graph_id)
            .await
            .unwrap();
        services.approval_queue().refresh();
        let approval_events = services
            .event_reader()
            .list_scope(runtime::RuntimeEventScope::Approval, 20)
            .unwrap();
        assert!(approval_events
            .iter()
            .any(|event| event.kind == "approval.submitted"));
        assert!(approval_events
            .iter()
            .any(|event| event.kind == "approval.decided"));
        assert_eq!(
            services
                .approval_queue()
                .get(&approval_id)
                .map(|request| request.status),
            Some(runtime::GlobalApprovalStatus::Approved)
        );
        let reconciled = services
            .graph_state_store()
            .load_async(graph_id)
            .await
            .unwrap();
        assert_eq!(
            reconciled.node_statuses.get(&node_id),
            Some(&ExecutionNodeStatus::Completed)
        );
        assert_eq!(
            services.approval_queue().get(&approval_id).unwrap().status,
            runtime::GlobalApprovalStatus::Approved
        );
    }

    fn review_principal(capabilities: &[&str]) -> runtime::VerifiedPrincipal {
        runtime::VerifiedPrincipal::from_test_claims(harness_contract::security::PrincipalClaims {
            principal_id: "reviewer".to_string(),
            kind: harness_contract::security::PrincipalKind::Human,
            scopes: vec!["gateway".to_string()],
            capabilities: capabilities
                .iter()
                .map(|capability| (*capability).to_string())
                .collect(),
            assurance: harness_contract::security::PrincipalAssurance::HumanInteractive,
            issuer: "test".to_string(),
            issued_at_ms: 1,
            expires_at_ms: None,
            credential_fingerprint: "test".to_string(),
            credential_epoch: 1,
            profile_revision: 1,
        })
    }

    #[tokio::test]
    async fn typed_application_approval_is_cropped_and_generic_response_fails_before_decision_write(
    ) {
        let services = runtime::RuntimeServices::in_memory().unwrap();
        let source = runtime::ApprovalSource {
            kind: runtime::ApprovalSourceKind::Application,
            session_id: None,
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: Some("application:report:crop".to_string()),
            review_ref: Some("review-crop".to_string()),
            application: Some(runtime::ApprovalApplicationSource {
                app_id: "fulfillment".to_string(),
                correlation_schema: "fulfillment.review.v1".to_string(),
                decision_capability: "fulfillment.review".to_string(),
            }),
        };
        let request = services
            .approval_queue()
            .submit_scoped(
                "application-approval:crop",
                runtime::SubmitGlobalApprovalRequest {
                    context: harness_contract::policy::ApprovalContext::owned(
                        &source,
                        "fulfillment.review.typed_decision",
                        "application:fulfillment",
                    ),
                    source,
                    action: "fulfillment.review.typed_decision".to_string(),
                    summary: "review report".to_string(),
                    risk: harness_contract::core::TaskRisk::High,
                    domain: harness_contract::policy::ApprovalDomain::Application,
                    blocks_execution: false,
                    evidence_refs: vec!["digest:crop".to_string()],
                    timeout_policy: runtime::ApprovalTimeoutPolicy::Pending,
                },
            )
            .unwrap();
        let service = ApprovalService::new().with_runtime_services(Arc::clone(&services));
        let operator = review_principal(&["approval.respond"]);
        let reviewer = review_principal(&["approval.respond", "fulfillment.review"]);
        assert_eq!(
            service.pending(&operator).await["pending"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            service.pending(&operator).await["approvals"]["requests"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            service.pending(&reviewer).await["pending"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            service
                .respond(
                    &request.approval_id,
                    true,
                    false,
                    runtime::ApprovalGrantScope::Once,
                    None,
                    &reviewer,
                )
                .await
                .unwrap_err(),
            "application_review_requires_typed_decision_service"
        );
        assert_eq!(
            services
                .approval_queue()
                .get(&request.approval_id)
                .unwrap()
                .status,
            runtime::GlobalApprovalStatus::Pending
        );
    }
}
