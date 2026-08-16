use std::sync::Arc;

use harness_contract::execution_graph::{ExecutionGraphCommand, ExecutionNodeStatus};
use harness_contract::policy::{PolicyDecisionKind, RiskAssessment, RiskGateReceipt, RiskLevel};
use runtime::{ApprovalConfig, ExecutionGraphHost};
use sha2::{Digest, Sha256};

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

    async fn approval_visible_to(
        &self,
        request: &runtime::GlobalApprovalRequest,
        principal: &runtime::VerifiedPrincipal,
    ) -> bool {
        if approval_admin(principal) {
            return true;
        }
        if !approval_base_visible_to(request, principal) {
            return false;
        }
        if request.source.typed_application().is_some() {
            return true;
        }
        let workspace_matches = self
            .runtime_services
            .as_deref()
            .is_some_and(|services| request.context.workspace_key == services.workspace_key());
        if !workspace_matches {
            return false;
        }
        let claims = principal.claims();
        if let Some(session_id) = request.context.session_id.as_deref() {
            let Some(runtime) = self.runtime.as_deref() else {
                return false;
            };
            return runtime
                .session_owner_principal_id(session_id)
                .await
                .as_deref()
                == Some(claims.principal_id.as_str());
        }
        request.context.principal_id == claims.principal_id
    }

    async fn approval_grant_visible_to(
        &self,
        grant: &harness_contract::policy::ApprovalGrant,
        principal: &runtime::VerifiedPrincipal,
    ) -> bool {
        if approval_admin(principal) {
            return true;
        }
        if !principal.is_human_interactive() || !principal.has_capability("approval.respond") {
            return false;
        }
        let workspace_matches = self
            .runtime_services
            .as_deref()
            .is_some_and(|services| grant.workspace_key == services.workspace_key());
        if !workspace_matches {
            return false;
        }
        if let Some(session_id) = grant.session_id.as_deref() {
            let Some(runtime) = self.runtime.as_deref() else {
                return false;
            };
            return runtime
                .session_owner_principal_id(session_id)
                .await
                .as_deref()
                == Some(principal.claims().principal_id.as_str());
        }
        grant.principal_id == principal.claims().principal_id
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
        let mut ids = Vec::new();
        for request in services.approval_queue().pending() {
            if self.approval_visible_to(&request, principal).await
                && request.created_at_ms < cutoff
                && request.status == runtime::GlobalApprovalStatus::Pending
            {
                ids.push(request.approval_id.clone());
            }
        }
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
        let Some(services) = self.runtime_services.as_deref() else {
            return serde_json::json!({
                "kind": "gateway.unified_approval_pending",
                "filter": {
                    "session_id": filter.session_id,
                    "domain": filter.domain.map(harness_contract::policy::ApprovalDomain::as_str),
                    "blocks_execution": filter.blocks_execution,
                },
                "pending": [],
                "approvals": serde_json::Value::Null,
            });
        };
        let mut requests = Vec::new();
        for request in services.approval_queue().list() {
            if !self.approval_visible_to(&request, principal).await
                || !filter.session_id.as_ref().is_none_or(|session_id| {
                    request.context.session_id.as_deref() == Some(session_id.as_str())
                })
                || !filter.domain.is_none_or(|domain| request.domain == domain)
                || !filter
                    .blocks_execution
                    .is_none_or(|blocks| request.blocks_execution == blocks)
            {
                continue;
            }
            requests.push(request);
        }
        let pending_requests = requests
            .iter()
            .filter(|request| request.status == runtime::GlobalApprovalStatus::Pending)
            .collect::<Vec<_>>();
        let pending = pending_requests
            .iter()
            .map(|request| project_approval_request(request, principal))
            .collect::<Vec<_>>();
        let mut grouped = std::collections::BTreeMap::<
            String,
            (
                harness_contract::policy::ApprovalEquivalenceKey,
                Vec<String>,
            ),
        >::new();
        for request in &pending_requests {
            let key = request.equivalence_key();
            grouped
                .entry(key.digest.clone())
                .or_insert_with(|| (key, Vec::new()))
                .1
                .push(request.approval_id.clone());
        }
        let groups = grouped
            .into_values()
            .map(|(key, mut approval_ids)| {
                approval_ids.sort();
                let count = approval_ids.len();
                let token_material = serde_json::json!({
                    "equivalence_digest": &key.digest,
                    "approval_ids": &approval_ids,
                });
                let batch_token = format!(
                    "approval-batch:{}",
                    format!(
                        "{:x}",
                        Sha256::digest(token_material.to_string().as_bytes())
                    )
                );
                serde_json::json!({
                    "equivalence_key": {
                        "digest": key.digest,
                        "domain": key.domain,
                        "risk": key.risk,
                        "blocks_execution": key.blocks_execution,
                    },
                    "approval_ids": approval_ids,
                    "count": count,
                    "batch_token": batch_token,
                    "batch_decision_supported": false,
                })
            })
            .collect::<Vec<_>>();
        let requests = requests
            .iter()
            .map(|request| project_approval_request(request, principal))
            .collect::<Vec<_>>();
        let projection = serde_json::json!({
            "kind": "runtime.global_approvals",
            "count": requests.len(),
            "pending_count": pending.len(),
            "requests": requests,
        });
        serde_json::json!({
            "kind": "gateway.unified_approval_pending",
            "filter": {
                "session_id": filter.session_id,
                "domain": filter.domain.map(harness_contract::policy::ApprovalDomain::as_str),
                "blocks_execution": filter.blocks_execution,
            },
            "pending": pending,
            "groups": groups,
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
            for request in services.approval_queue().list() {
                if request.status != runtime::GlobalApprovalStatus::Pending
                    && self.approval_visible_to(&request, principal).await
                {
                    combined.push(project_approval_request(&request, principal));
                }
            }
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
                .find(|request| request.approval_id == id)
            {
                if self.approval_visible_to(&request, principal).await {
                    return Some(project_approval_request(&request, principal));
                }
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
        let mut grants = Vec::new();
        for grant in services.approval_queue().grants() {
            if self.approval_grant_visible_to(&grant, principal).await {
                grants.push(grant);
            }
        }
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
        let existing = services
            .approval_queue()
            .grants()
            .into_iter()
            .find(|grant| grant.grant_id == grant_id)
            .ok_or_else(|| format!("approval grant not found: {grant_id}"))?;
        if !self.approval_grant_visible_to(&existing, principal).await {
            return Err("approval grant is outside the authenticated principal scope".to_string());
        }
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
        let request = services
            .approval_queue()
            .get(id)
            .ok_or_else(|| format!("approval_not_found: {id}"))?;
        if !self.approval_visible_to(&request, principal).await {
            return Err("approval_outside_authenticated_principal_scope".to_string());
        }
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
        let decision = runtime::ApprovalDecisionCommand {
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
        };
        let graph_receipt = if !skip {
            if let (Some((graph_id, node_id)), Some(graph)) = (graph_target, graph_before) {
                let command = ExecutionGraphCommand::SubmitApproval {
                    expected_revision: graph.revision,
                    node_id: node_id.clone(),
                    decision: Box::new(decision.clone()),
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
            serde_json::to_value(services.approval_queue().decide(principal, decision)?)
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

fn approval_base_visible_to(
    request: &runtime::GlobalApprovalRequest,
    principal: &runtime::VerifiedPrincipal,
) -> bool {
    if approval_admin(principal) {
        return true;
    }
    if !principal.is_human_interactive() || !principal.has_capability("approval.respond") {
        return false;
    }
    let claims = principal.claims();
    if let Some(application) = request.source.typed_application() {
        return principal.has_capability(&application.decision_capability)
            && request
                .source
                .resource_ref
                .as_deref()
                .is_some_and(|resource| {
                    claims.scopes.iter().any(|scope| {
                        scope == "gateway"
                            || scope == resource
                            || resource.starts_with(&format!("{scope}:"))
                    })
                });
    }
    true
}

fn approval_admin(principal: &runtime::VerifiedPrincipal) -> bool {
    principal.is_human_interactive()
        && (principal.has_capability("approval.manage")
            || principal.has_capability("runtime.maintenance.manage"))
}

fn project_approval_request(
    request: &runtime::GlobalApprovalRequest,
    principal: &runtime::VerifiedPrincipal,
) -> serde_json::Value {
    let mut value = serde_json::to_value(request).unwrap_or(serde_json::Value::Null);
    let graph_owned = canonical_graph_approval_target(&request.approval_id).is_some();
    let declares_mutation = request.context.effect.as_ref().is_some_and(|effect| {
        effect.required_permission != harness_contract::policy::PermissionMode::ReadOnly
            || effect.effect_kind != harness_contract::tool::ToolEffectKind::Read
    });
    if let Some(object) = value.as_object_mut() {
        // These typed aliases intentionally live only in the authenticated
        // Gateway projection. Runtime remains the durable owner of the
        // underlying context and policy snapshot while every Surface consumes
        // one stable wire shape.
        object.insert(
            "skippable".to_string(),
            serde_json::json!(request.skippable && graph_owned && !declares_mutation),
        );
        object.insert(
            "policy_revision".to_string(),
            serde_json::json!(request.context.policy_revision),
        );
        object.insert(
            "approval_profile".to_string(),
            serde_json::to_value(request.context.approval_profile)
                .unwrap_or(serde_json::Value::Null),
        );
        object.insert(
            "requested_sandbox_posture".to_string(),
            serde_json::to_value(request.context.requested_sandbox_posture)
                .unwrap_or(serde_json::Value::Null),
        );
        object.insert(
            "effective_sandbox_posture".to_string(),
            serde_json::to_value(request.context.effective_sandbox_posture)
                .unwrap_or(serde_json::Value::Null),
        );
        object.insert(
            "effect".to_string(),
            serde_json::to_value(&request.context.effect).unwrap_or(serde_json::Value::Null),
        );
        object.insert(
            "equivalence_key".to_string(),
            serde_json::json!({"digest": request.equivalence_key().digest}),
        );
    }
    if approval_admin(principal) {
        return value;
    }
    let Some(object) = value.as_object_mut() else {
        return value;
    };
    object.insert("evidence_refs".to_string(), serde_json::json!([]));
    if let Some(context) = object
        .get_mut("context")
        .and_then(serde_json::Value::as_object_mut)
    {
        context.insert("principal_id".to_string(), serde_json::json!("self"));
        context.insert("workspace_key".to_string(), serde_json::json!("current"));
        if let Some(effect) = context
            .get_mut("effect")
            .and_then(serde_json::Value::as_object_mut)
        {
            effect.remove("descriptor_hash");
        }
    }
    if let Some(effect) = object
        .get_mut("effect")
        .and_then(serde_json::Value::as_object_mut)
    {
        effect.remove("descriptor_hash");
    }
    value
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
                    decision: Box::new(runtime::ApprovalDecisionCommand {
                        approval_id: approval_id.clone(),
                        approved: true,
                        skip: false,
                        reason: "approved in graph atomicity test".to_string(),
                        scope: harness_contract::policy::ApprovalGrantScope::Once,
                        actor: harness_contract::policy::ApprovalDecisionActor {
                            kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                            actor_id: "test-human".to_string(),
                        },
                        evidence_refs: vec!["test.graph.atomic_approval".to_string()],
                    }),
                },
            )
            .await
            .unwrap();
        services
            .execution_supervisor()
            .wait_for_quiescence(&graph_id)
            .await
            .unwrap();
        // The graph transaction updates the durable approval stream before the
        // queue read model is refreshed. A concurrent deadline path must honor
        // that durable winner instead of appending a stale timeout decision.
        let timeout_receipt = services.approval_queue().timeout(&approval_id).unwrap();
        assert_eq!(
            timeout_receipt.status,
            runtime::GlobalApprovalStatus::Approved
        );
        assert_eq!(services.approval_queue().active_request_count(), 0);
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
        assert!(!approval_events
            .iter()
            .any(|event| event.kind == "approval.timed_out"));
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
        let request = services.approval_queue().get(&approval_id).unwrap();
        let decision = request.decision.expect("typed graph decision");
        assert_eq!(decision.scope, runtime::ApprovalGrantScope::Once);
        assert_eq!(decision.actor.actor_id, "test-human");
        assert_eq!(decision.reason, "approved in graph atomicity test");
        assert!(services.approval_queue().grants().iter().any(|grant| {
            grant.approval_id == approval_id
                && grant.scope == runtime::ApprovalGrantScope::Once
                && grant.issued_by.actor_id == "test-human"
        }));
    }

    fn review_principal(capabilities: &[&str]) -> runtime::VerifiedPrincipal {
        runtime::VerifiedPrincipal::from_test_claims(harness_contract::security::PrincipalClaims {
            principal_id: "reviewer".to_string(),
            tenant_id: "tenant:test".to_string(),
            grant_id: "grant:reviewer".to_string(),
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
            app_profiles: std::collections::BTreeMap::new(),
        })
    }

    fn approval_projection_fixture(
        effect_kind: harness_contract::tool::ToolEffectKind,
        required_permission: harness_contract::policy::PermissionMode,
        skippable: bool,
    ) -> runtime::GlobalApprovalRequest {
        let source = runtime::ApprovalSource {
            kind: runtime::ApprovalSourceKind::Session,
            session_id: Some("projection-session".to_string()),
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: Some("workspace:file.txt".to_string()),
            review_ref: None,
            application: None,
        };
        let policy = harness_contract::policy::SessionExecutionPolicy {
            autonomy_profile: harness_contract::policy::AutonomyProfileId::Supervised,
            permission_mode: harness_contract::policy::PermissionMode::WorkspaceWrite,
            sandbox_posture: harness_contract::policy::SandboxPosture::WorkspaceWriteSandbox,
            approval_profile: harness_contract::policy::ApprovalProfile::Balanced,
            interruption_policy: harness_contract::policy::InterruptionPolicy::PauseOnRisk,
            revision: 7,
            origin: harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
        };
        let mut context = harness_contract::policy::ApprovalContext::owned(
            &source,
            "bash",
            "workspace:projection",
        )
        .with_execution_policy(&policy);
        context.effect = Some(harness_contract::tool::ToolEffectDescriptor {
            tool_id: "bash".to_string(),
            descriptor_hash: "private-effect-hash".to_string(),
            effect_kind,
            idempotency: harness_contract::tool::ToolIdempotency::IdempotentWithKey,
            scopes: vec![harness_contract::policy::PermissionScope::new(
                harness_contract::policy::PermissionResource::File,
                if required_permission == harness_contract::policy::PermissionMode::ReadOnly {
                    harness_contract::policy::PermissionOperation::Read
                } else {
                    harness_contract::policy::PermissionOperation::Write
                },
            )],
            required_permission,
            approval_class: harness_contract::tool::ToolApprovalClass::User,
            uses_network: false,
            spawns_process: true,
            mutates_packages: false,
            mutates_system: false,
            assessment: Default::default(),
        });
        runtime::GlobalApprovalRequest {
            approval_id: runtime::execution_core::graph::executors::graph_approval_id(
                "projection-graph",
                "projection-node",
            ),
            source,
            context,
            action: "bash".to_string(),
            summary: "review the exact workspace operation".to_string(),
            risk: harness_contract::core::TaskRisk::High,
            domain: harness_contract::policy::ApprovalDomain::Execution,
            blocks_execution: true,
            skippable,
            allowed_scopes: vec![
                harness_contract::policy::ApprovalGrantScope::Once,
                harness_contract::policy::ApprovalGrantScope::Session,
            ],
            evidence_refs: vec!["private:evidence".to_string()],
            timeout_policy: harness_contract::policy::ApprovalTimeoutPolicy::Pending,
            status: runtime::GlobalApprovalStatus::Pending,
            decision: None,
            created_at_ms: 10,
            expires_at_ms: Some(20),
            resolved_at_ms: None,
        }
    }

    #[test]
    fn projection_exposes_typed_policy_fields_and_only_skips_graph_reads() {
        let admin = review_principal(&["approval.manage"]);
        let read = approval_projection_fixture(
            harness_contract::tool::ToolEffectKind::Read,
            harness_contract::policy::PermissionMode::ReadOnly,
            true,
        );
        let projected = project_approval_request(&read, &admin);
        assert_eq!(projected["skippable"], true);
        assert_eq!(
            projected["allowed_scopes"],
            serde_json::json!(["once", "session"])
        );
        assert_eq!(projected["policy_revision"], 7);
        assert_eq!(
            projected["requested_sandbox_posture"],
            "workspace_write_sandbox"
        );
        assert_eq!(
            projected["effective_sandbox_posture"],
            "workspace_write_sandbox"
        );
        assert_eq!(projected["expires_at_ms"], 20);
        assert_eq!(projected["effect"]["effect_kind"], "read");
        let read_group = projected["equivalence_key"]["digest"]
            .as_str()
            .expect("server-derived equivalence digest")
            .to_string();
        assert_eq!(
            read.equivalence_key().digest,
            approval_projection_fixture(
                harness_contract::tool::ToolEffectKind::Read,
                harness_contract::policy::PermissionMode::ReadOnly,
                true,
            )
            .equivalence_key()
            .digest,
            "equivalent approvals must group deterministically"
        );

        let write = approval_projection_fixture(
            harness_contract::tool::ToolEffectKind::Write,
            harness_contract::policy::PermissionMode::WorkspaceWrite,
            true,
        );
        let write_projected = project_approval_request(&write, &admin);
        assert_eq!(write_projected["skippable"], false);
        assert_ne!(
            write_projected["equivalence_key"]["digest"], read_group,
            "read and write approval boundaries must never visually coalesce"
        );

        let mut non_graph = read;
        non_graph.approval_id = "session-approval:not-a-graph".to_string();
        assert_eq!(
            project_approval_request(&non_graph, &admin)["skippable"],
            false
        );
    }

    #[test]
    fn ordinary_projection_preserves_decision_context_but_redacts_authority_secrets() {
        let owner = review_principal(&["approval.respond"]);
        let request = approval_projection_fixture(
            harness_contract::tool::ToolEffectKind::Write,
            harness_contract::policy::PermissionMode::WorkspaceWrite,
            false,
        );
        let projected = project_approval_request(&request, &owner);
        assert_eq!(projected["summary"], request.summary);
        assert_eq!(
            projected["context"]["resource_targets"],
            serde_json::json!(["workspace:file.txt"])
        );
        assert_eq!(projected["evidence_refs"], serde_json::json!([]));
        assert!(projected["effect"].get("descriptor_hash").is_none());
        assert!(projected["context"]["effect"]
            .get("descriptor_hash")
            .is_none());
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

    #[tokio::test]
    async fn prune_denies_only_overdue_pending_and_skips_decided() {
        let services = runtime::RuntimeServices::in_memory().unwrap();
        let service = ApprovalService::new().with_runtime_services(Arc::clone(&services));
        let operator = review_principal(&["approval.respond", "runtime.maintenance.manage"]);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let old = now_ms.saturating_sub(40 * 86_400_000);

        let submit = |approval_id: &str, source: runtime::ApprovalSource| {
            services
                .approval_queue()
                .submit_scoped(
                    approval_id,
                    runtime::SubmitGlobalApprovalRequest {
                        context: harness_contract::policy::ApprovalContext::owned(
                            &source,
                            "prune.test",
                            approval_id,
                        ),
                        source,
                        action: "prune.test".to_string(),
                        summary: "prune test approval".to_string(),
                        risk: harness_contract::core::TaskRisk::Medium,
                        domain: harness_contract::policy::ApprovalDomain::Execution,
                        blocks_execution: true,
                        evidence_refs: Vec::new(),
                        timeout_policy: runtime::ApprovalTimeoutPolicy::Pending,
                    },
                )
                .expect("submit approval")
        };
        let session_source = |suffix: &str| runtime::ApprovalSource {
            kind: runtime::ApprovalSourceKind::Session,
            session_id: Some(format!("prune-session-{suffix}")),
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: None,
            review_ref: None,
            application: None,
        };
        let pending = submit("prune-pending-1", session_source("pending"));
        services
            .approval_queue()
            .backdate_created_at_for_test(&pending.approval_id, old)
            .expect("backdate pending");
        let decided = submit("prune-decided-1", session_source("decided"));
        services
            .approval_queue()
            .backdate_created_at_for_test(&decided.approval_id, old)
            .expect("backdate decided before its terminal decision");
        service
            .respond(
                &decided.approval_id,
                true,
                false,
                runtime::ApprovalGrantScope::Once,
                Some("approved before prune".to_string()),
                &operator,
            )
            .await
            .expect("decide approval");
        let result = service
            .prune(30, Some("housekeeping".to_string()), &operator)
            .await
            .expect("prune");

        assert_eq!(result["pruned"], 1);
        assert_eq!(result["failed"], 0);
        assert_eq!(
            services
                .approval_queue()
                .get(&pending.approval_id)
                .map(|request| request.status),
            Some(runtime::GlobalApprovalStatus::Denied)
        );
        assert_eq!(
            services
                .approval_queue()
                .get(&decided.approval_id)
                .map(|request| request.status),
            Some(runtime::GlobalApprovalStatus::Approved)
        );
    }

    #[tokio::test]
    async fn prune_collects_respond_failures_without_losing_audit() {
        let services = runtime::RuntimeServices::in_memory().unwrap();
        let service = ApprovalService::new().with_runtime_services(Arc::clone(&services));
        let operator = review_principal(&["approval.respond", "fulfillment.review"]);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let old = now_ms.saturating_sub(40 * 86_400_000);
        let source = runtime::ApprovalSource {
            kind: runtime::ApprovalSourceKind::Application,
            session_id: None,
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: Some("application:report:prune".to_string()),
            review_ref: Some("review-prune".to_string()),
            application: Some(runtime::ApprovalApplicationSource {
                app_id: "fulfillment".to_string(),
                correlation_schema: "fulfillment.review.v1".to_string(),
                decision_capability: "fulfillment.review".to_string(),
            }),
        };
        let pending = services
            .approval_queue()
            .submit_scoped(
                "prune-application-failure",
                runtime::SubmitGlobalApprovalRequest {
                    context: harness_contract::policy::ApprovalContext::owned(
                        &source,
                        "fulfillment.review.typed_decision",
                        "prune-application-failure",
                    ),
                    source,
                    action: "fulfillment.review.typed_decision".to_string(),
                    summary: "typed application approval".to_string(),
                    risk: harness_contract::core::TaskRisk::High,
                    domain: harness_contract::policy::ApprovalDomain::Application,
                    blocks_execution: false,
                    evidence_refs: Vec::new(),
                    timeout_policy: runtime::ApprovalTimeoutPolicy::Pending,
                },
            )
            .expect("submit application approval");
        services
            .approval_queue()
            .backdate_created_at_for_test(&pending.approval_id, old)
            .expect("backdate");

        let result = service
            .prune(30, Some("housekeeping".to_string()), &operator)
            .await
            .expect("prune");

        assert_eq!(result["pruned"], 0);
        assert_eq!(result["failed"], 1);
        assert!(
            result["failures"][0]
                .as_str()
                .is_some_and(|value| value.contains(&pending.approval_id)),
            "failure must name the approval id"
        );
        assert_eq!(
            services
                .approval_queue()
                .get(&pending.approval_id)
                .map(|request| request.status),
            Some(runtime::GlobalApprovalStatus::Pending)
        );
    }
}
