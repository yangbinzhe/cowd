use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
};

use runtime::{
    ConnectorActionContext, ConnectorRegistrySnapshot, CrossPlaneAction, CrossPlaneAuditRecord,
    CrossPlaneControlPlane, CrossPlaneDecisionEvidence, CrossPlaneDispatchOutcome,
    CrossPlaneDispatchTarget, CrossPlaneExecutionReceipt, CrossPlanePolicyDecision,
    ProviderAccount,
};

use super::{CrossPlaneService, ServiceEnvelope};

static CROSS_PLANE_CONTROL: OnceLock<CrossPlaneControlPlane> = OnceLock::new();
static CROSS_PLANE_LOADED: OnceLock<()> = OnceLock::new();

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
}

impl CrossPlaneService {
    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: "service_ready",
            owner: self.owner,
            boundary_status: "0620_final_boundary",
        }
    }

    pub(crate) fn control(&self) -> &'static CrossPlaneControlPlane {
        CROSS_PLANE_CONTROL.get_or_init(CrossPlaneControlPlane::new)
    }

    pub(crate) fn ensure_loaded(&self, config_home: impl AsRef<Path>) {
        let path = self.state_path(config_home);
        let _ = CROSS_PLANE_LOADED.get_or_init(|| {
            let _ = self.control().load_from_path(&path);
        });
    }

    pub(crate) fn save_state(&self, config_home: impl AsRef<Path>) {
        let _ = self.control().save_to_path(&self.state_path(config_home));
    }

    pub(crate) fn find_execution_by_idempotency_key(
        &self,
        idempotency_key: &str,
    ) -> Option<CrossPlaneExecutionReceipt> {
        self.control()
            .find_execution_by_idempotency_key(idempotency_key)
    }

    pub(crate) fn consume_matched_grant_for_decision(
        &self,
        decision: &CrossPlanePolicyDecision,
    ) -> Option<(String, u32)> {
        self.control().consume_matched_grant_for_decision(decision)
    }

    pub(crate) fn record_action_execution(
        &self,
        record: CrossPlaneExecutionRecord,
    ) -> (String, CrossPlaneExecutionReceipt) {
        let audit_record = CrossPlaneAuditRecord::new(
            record.action.clone(),
            record.decision.clone(),
            record.audit_result,
            record.audit_summary,
        )
        .with_evidence(record.evidence);
        let audit_record_id = audit_record.id.clone();
        self.control().record_audit(audit_record);

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
        .with_dispatch_outcome(record.dispatch_outcome);
        self.control().record_execution(receipt.clone());
        (audit_record_id, receipt)
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
        self.control()
            .decide_with_connector_context(action, connector_context, now)
    }

    fn state_path(&self, config_home: impl AsRef<Path>) -> PathBuf {
        config_home
            .as_ref()
            .join("cross-plane")
            .join("control-state.json")
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
