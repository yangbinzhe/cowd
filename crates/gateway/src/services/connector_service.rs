use std::path::{Path, PathBuf};

use connector::{ExternalResourceRef, SqliteResourceDirectory};
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
    ) -> CrossPlaneExecutionReceipt {
        let audit_result = if mode == "commit" && status == "executed" {
            "executed"
        } else if status == "dry_run" {
            "dry_run"
        } else {
            "blocked"
        };
        let (_, receipt) = cross_plane.record_action_execution(CrossPlaneExecutionRecord {
            idempotency_key,
            mode: mode.to_string(),
            status: match status {
                "executed" => "executed",
                "dry_run" => "dry_run",
                _ => "blocked",
            }
            .to_string(),
            dispatch_status: match dispatch_status {
                "service_mock_executed" => "service_mock_executed",
                "service_feishu_readonly_resolved" => "service_feishu_readonly_resolved",
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
        });
        receipt
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
    ) -> rusqlite::Result<SqliteResourceDirectory> {
        let handle = self.resource_directory_handle(workspace_root);
        if let Some(parent) = handle.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                    error.kind(),
                    format!("failed to create resource directory parent: {error}"),
                )))
            })?;
        }
        SqliteResourceDirectory::open_storage_handle(&handle)
    }

    pub(crate) fn resource_directory_handle(
        &self,
        workspace_root: impl AsRef<Path>,
    ) -> storage::StorageHandle {
        let config_home = workspace_root.as_ref().join(".cowd");
        storage::StorageRegistry::default_for_config_home(config_home)
            .sqlite_handle("resource_directory")
            .cloned()
            .unwrap_or_else(|_| {
                storage::StorageHandle::sqlite(
                    "resource_directory",
                    self.resource_directory_path(workspace_root),
                    "connector",
                    "workspace_scoped_storage_handle_since_0.9.315",
                )
            })
    }

    pub(crate) fn resource_directory_path(&self, workspace_root: impl AsRef<Path>) -> PathBuf {
        workspace_root
            .as_ref()
            .join(".cowd")
            .join("storage")
            .join("resource-directory.sqlite")
    }

    pub(crate) fn list_resources(
        &self,
        workspace_root: impl AsRef<Path>,
        limit: usize,
        offset: usize,
        query: Option<&str>,
    ) -> rusqlite::Result<Vec<ExternalResourceRef>> {
        let directory = self.resource_directory(workspace_root)?;
        query
            .map(|value| directory.search(value, limit))
            .unwrap_or_else(|| directory.list_page(limit, offset))
    }

    pub(crate) fn recent_resources(
        &self,
        workspace_root: impl AsRef<Path>,
        limit: usize,
    ) -> rusqlite::Result<Vec<ExternalResourceRef>> {
        self.resource_directory(workspace_root)?.list_recent(limit)
    }

    pub(crate) fn search_resources(
        &self,
        workspace_root: impl AsRef<Path>,
        query: &str,
        limit: usize,
    ) -> rusqlite::Result<Vec<ExternalResourceRef>> {
        self.resource_directory(workspace_root)?
            .search(query, limit)
    }

    pub(crate) fn get_resource(
        &self,
        workspace_root: impl AsRef<Path>,
        reference: &str,
    ) -> rusqlite::Result<Option<ExternalResourceRef>> {
        self.resource_directory(workspace_root)?.get(reference)
    }

    pub(crate) fn upsert_resource(
        &self,
        workspace_root: impl AsRef<Path>,
        resource: &ExternalResourceRef,
    ) -> rusqlite::Result<()> {
        self.resource_directory(workspace_root)?
            .upsert(resource)
            .map(|_| ())
    }

    pub(crate) fn mark_resource_state(
        &self,
        workspace_root: impl AsRef<Path>,
        reference: &str,
        desired_state: &str,
    ) -> rusqlite::Result<(bool, Option<ExternalResourceRef>, Option<String>)> {
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
