use super::*;

fn default_cross_plane_capability(execution: &MfgActionExecution) -> &'static str {
    match execution.action_type.as_str() {
        "supplier_recovery" | "plan_bom_reconciliation" | "evidence_review" => {
            "channel.feishu.send_text"
        }
        _ => "channel.feishu.send_text",
    }
}

fn default_bridge_message(execution: &MfgActionExecution) -> String {
    format!(
        "MFG action {} [{}]: {}; incident={}; execution={}",
        execution.action_type,
        execution.owner_role,
        execution.title,
        execution.incident_id,
        execution.execution_id
    )
}

fn cross_plane_risk(execution: &MfgActionExecution) -> CrossPlaneRisk {
    if execution.governance.contains("human_review") || execution.mode == "commit" {
        CrossPlaneRisk::Medium
    } else {
        CrossPlaneRisk::Low
    }
}

impl MfgService {
    pub(crate) fn cross_plane_action_from_execution(
        &self,
        execution: &MfgActionExecution,
        request: &MfgCrossPlaneBridgeRequest,
    ) -> CrossPlaneAction {
        let actor_principal = request
            .actor_principal
            .as_deref()
            .or(execution.operator_id.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("mfg:operator");
        let requested_capability = request
            .requested_capability
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| default_cross_plane_capability(execution));
        let mut action = CrossPlaneAction::new(actor_principal, requested_capability);
        action.actor_identity_ref = request.actor_identity_ref.clone();
        action.source_channel = Some(
            request
                .source_channel
                .clone()
                .unwrap_or_else(|| "mfg".to_string()),
        );
        action.session_id = Some(execution.incident_id.clone());
        action.provider_account = request.provider_account.clone();
        action.target_ref = request.target_ref.clone();
        action.resource_ref = request
            .resource_ref
            .clone()
            .or_else(|| Some(format!("text://{}", default_bridge_message(execution))));
        action.risk = cross_plane_risk(execution);
        action.data_classification = DataClassification::Internal;
        action.identity_trust = IdentityTrust::Unknown;
        action
    }

    pub(crate) fn bridge_outcome(
        &self,
        mode: &str,
        decision: &CrossPlanePolicyDecision,
    ) -> (String, String, Vec<String>, String, String) {
        if decision.decision == PolicyDecisionKind::Allow {
            if mode == "dry_run" {
                return (
                    "planned".to_string(),
                    "dry_run".to_string(),
                    Vec::new(),
                    "dry_run".to_string(),
                    "mfg_cross_plane_bridge_dry_run_plan".to_string(),
                );
            }
            return (
                "planned".to_string(),
                "human_review_required".to_string(),
                vec!["mfg:human_review_required".to_string()],
                "planned".to_string(),
                "mfg_cross_plane_bridge_queued_for_human_review".to_string(),
            );
        }
        (
            "blocked".to_string(),
            "policy_blocked".to_string(),
            vec![format!("policy:{}", decision.reason)],
            "blocked".to_string(),
            "mfg_cross_plane_bridge_policy_blocked".to_string(),
        )
    }

    pub(crate) fn record_cross_plane_bridge_receipt(
        &self,
        cross_plane: &CrossPlaneService,
        idempotency_key: Option<String>,
        mode: String,
        action: CrossPlaneAction,
        decision: CrossPlanePolicyDecision,
        evidence: CrossPlaneDecisionEvidence,
    ) -> CrossPlaneExecutionReceipt {
        let (status, dispatch_status, blockers, audit_result, audit_summary) =
            self.bridge_outcome(&mode, &decision);
        let (_, receipt) = cross_plane.record_action_execution(CrossPlaneExecutionRecord {
            idempotency_key,
            mode,
            status: match status.as_str() {
                "planned" => "planned",
                "blocked" => "blocked",
                _ => "blocked",
            }
            .to_string(),
            dispatch_status: match dispatch_status.as_str() {
                "dry_run" => "dry_run",
                "human_review_required" => "human_review_required",
                "policy_blocked" => "policy_blocked",
                _ => "not_dispatched",
            }
            .to_string(),
            action,
            decision,
            blockers,
            dispatch_target: None,
            dispatch_outcome: None,
            evidence,
            audit_result,
            audit_summary,
        });
        receipt
    }
}
