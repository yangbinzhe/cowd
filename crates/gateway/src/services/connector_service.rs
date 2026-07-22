use std::path::Path;

use connector::{ExternalResourceRef, ResourceDirectoryRepository, ResourceDirectoryResult};
use runtime::{
    CrossPlaneAction, CrossPlaneDecisionEvidence, CrossPlaneExecutionReceipt,
    CrossPlanePolicyDecision, PolicyDecisionKind,
};

use super::{ConnectorService, CrossPlaneExecutionRecord, CrossPlaneService, ServiceEnvelope};

impl ConnectorService {
    pub(crate) fn service_action(
        &self,
        actor_principal: String,
        tool_id: String,
        actor_identity_ref: Option<String>,
        source_channel: Option<String>,
        session_id: Option<String>,
        provider_account: impl Into<String>,
        resource_ref: Option<String>,
    ) -> CrossPlaneAction {
        let mut action = CrossPlaneAction::new(actor_principal, tool_id);
        action.actor_identity_ref = actor_identity_ref;
        action.source_channel = source_channel;
        action.session_id = session_id;
        action.provider_account = Some(provider_account.into());
        action.resource_ref = resource_ref;
        action
    }

    pub(crate) fn policy_allows(&self, decision: &CrossPlanePolicyDecision) -> bool {
        decision.decision == PolicyDecisionKind::Allow
    }

    pub(crate) fn record_service_execution_receipt(
        &self,
        cross_plane: &CrossPlaneService,
        idempotency_key: Option<String>,
        mode: &str,
        status: &str,
        dispatch_status: &str,
        action: CrossPlaneAction,
        decision: CrossPlanePolicyDecision,
        blockers: Vec<String>,
        evidence: CrossPlaneDecisionEvidence,
        audit_summary: String,
        execution_graph_id: Option<String>,
    ) -> Result<CrossPlaneExecutionReceipt, runtime::CrossPlaneRuntimeError> {
        let audit_result = if mode == "commit" && status == "executed" {
            "executed"
        } else if status == "dry_run" {
            "dry_run"
        } else {
            "blocked"
        };
        let record = CrossPlaneExecutionRecord {
            idempotency_key,
            mode: mode.to_string(),
            status: match status {
                "executed" => "executed",
                "dry_run" => "dry_run",
                _ => "blocked",
            }
            .to_string(),
            dispatch_status: match dispatch_status {
                "service_executed" | "service_mock_executed" => dispatch_status,
                _ => "not_dispatched",
            }
            .to_string(),
            action,
            decision,
            blockers,
            dispatch_target: None,
            dispatch_outcome: None,
            evidence,
            audit_result: audit_result.to_string(),
            audit_summary,
            execution_graph_id,
        };
        let (_, receipt) = if mode == "commit" && status == "executed" {
            cross_plane.record_completed_effect_execution(record)?
        } else {
            cross_plane.record_action_execution(record)?
        };
        Ok(receipt)
    }

    pub(crate) fn resource_list(&self) -> ServiceEnvelope {
        self.envelope("resource_list")
    }

    pub(crate) fn resource_revalidate(&self) -> ServiceEnvelope {
        self.envelope("resource_revalidate")
    }

    pub(crate) fn resource_promote_memory(&self) -> ServiceEnvelope {
        self.envelope("resource_promote_memory")
    }

    pub(crate) fn resource_directory(
        &self,
        workspace_root: impl AsRef<Path>,
    ) -> ResourceDirectoryResult<std::sync::Arc<dyn ResourceDirectoryRepository>> {
        let handle = self.resource_directory_handle(workspace_root);
        self.resource_directory_factory.open(&handle)
    }

    pub(crate) fn resource_directory_handle(
        &self,
        workspace_root: impl AsRef<Path>,
    ) -> storage::StorageHandle {
        let workspace_root = workspace_root.as_ref();
        let scope = storage::StorageScope::workspace_for_root(workspace_root);
        storage::StorageRegistry::default_for_config_home(workspace_root.join(".cowd"))
            .with_workspace(workspace_root)
            .and_then(|registry| {
                registry
                    .endpoint_in_scope(&storage::StorageDomainId::ConnectorDirectory, &scope)
                    .map(storage::StorageEndpoint::as_handle)
            })
            .expect("workspace connector endpoint registration must be valid")
    }

    pub(crate) fn resource_directory_initialized(&self, workspace_root: impl AsRef<Path>) -> bool {
        let handle = self.resource_directory_handle(workspace_root);
        self.resource_directory_factory.is_initialized(&handle)
    }

    pub(crate) fn list_resources(
        &self,
        workspace_root: impl AsRef<Path>,
        limit: usize,
        offset: usize,
        query: Option<&str>,
    ) -> ResourceDirectoryResult<Vec<ExternalResourceRef>> {
        let directory = self.resource_directory(workspace_root)?;
        query
            .map(|value| directory.search(value, limit))
            .unwrap_or_else(|| directory.list_page(limit, offset))
    }

    pub(crate) fn recent_resources(
        &self,
        workspace_root: impl AsRef<Path>,
        limit: usize,
    ) -> ResourceDirectoryResult<Vec<ExternalResourceRef>> {
        self.resource_directory(workspace_root)?.list_recent(limit)
    }

    pub(crate) fn search_resources(
        &self,
        workspace_root: impl AsRef<Path>,
        query: &str,
        limit: usize,
    ) -> ResourceDirectoryResult<Vec<ExternalResourceRef>> {
        self.resource_directory(workspace_root)?
            .search(query, limit)
    }

    pub(crate) fn get_resource(
        &self,
        workspace_root: impl AsRef<Path>,
        reference: &str,
    ) -> ResourceDirectoryResult<Option<ExternalResourceRef>> {
        self.resource_directory(workspace_root)?.get(reference)
    }

    pub(crate) fn upsert_resource(
        &self,
        workspace_root: impl AsRef<Path>,
        resource: &ExternalResourceRef,
    ) -> ResourceDirectoryResult<()> {
        self.resource_directory(workspace_root)?
            .upsert(resource)
            .map(|_| ())
    }

    pub(crate) fn mark_resource_state(
        &self,
        workspace_root: impl AsRef<Path>,
        reference: &str,
        desired_state: &str,
    ) -> ResourceDirectoryResult<(bool, Option<ExternalResourceRef>, Option<String>)> {
        let directory = self.resource_directory(workspace_root)?;
        let changed = match desired_state {
            "indexed" => directory.mark_indexed(reference)?,
            "stale" => directory.mark_stale(reference)?,
            other => return Ok((false, None, Some(format!("unsupported state: {other}")))),
        };
        let resource = directory.get(reference)?;
        Ok((changed, resource, None))
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.resource_list(),
            self.resource_revalidate(),
            self.resource_promote_memory(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_connector_service_uses_durable_directory_port() {
        let workspace = tempfile::tempdir().expect("temporary workspace");
        let service = ConnectorService::new();
        let resource =
            ExternalResourceRef::new("feishu", "bitable", "gateway-port", "Gateway port");
        service
            .upsert_resource(workspace.path(), &resource)
            .expect("upsert through connector port");
        let (changed, persisted, reason) = service
            .mark_resource_state(workspace.path(), &resource.reference, "indexed")
            .expect("mark state through connector port");
        assert!(changed);
        assert!(reason.is_none());
        assert_eq!(persisted.expect("resource exists").indexed_state, "indexed");
    }
}
