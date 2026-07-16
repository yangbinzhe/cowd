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
        let actor_principal = request.actor_principal.trim();
        debug_assert!(
            !actor_principal.is_empty(),
            "Gateway must inject the verified principal before MFG dispatch"
        );
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

    pub(crate) fn execution_bridge_receipt_matches(
        &self,
        receipt: &CrossPlaneExecutionReceipt,
        requested_action: &CrossPlaneAction,
    ) -> bool {
        receipt.action == *requested_action
    }
}
