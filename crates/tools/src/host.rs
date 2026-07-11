//! Workspace-scoped tool implementation host.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use harness_contract::tool::{
    ToolDiscoveryReceipt, ToolEffectDescriptor, ToolExecutionAuthorization, ToolIdempotency,
    ToolPermissionMode,
};
use mcp::McpService;
use serde_json::Value;

use crate::lsp_client::LspRegistry;
use crate::tool_cache::{ToolCache, ToolCacheStats};
use crate::tool_orchestrator::describe_tool_effect;
use crate::ToolCatalog;

/// Immutable implementation snapshot pinned for one request.
#[derive(Clone)]
pub struct ToolHostSnapshot {
    pub catalog: Arc<ToolCatalog>,
    pub lsp: Arc<LspRegistry>,
    pub mcp: Option<Arc<dyn McpService>>,
    pub descriptor_set_hash: String,
}

impl std::fmt::Debug for ToolHostSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolHostSnapshot")
            .field("tool_count", &self.catalog.definitions(None).len())
            .field("lsp_count", &self.lsp.len())
            .field("mcp_configured", &self.mcp.is_some())
            .field("descriptor_set_hash", &self.descriptor_set_hash)
            .finish()
    }
}

impl ToolHostSnapshot {
    #[must_use]
    pub fn new(
        catalog: Arc<ToolCatalog>,
        lsp: Arc<LspRegistry>,
        mcp: Option<Arc<dyn McpService>>,
    ) -> Self {
        let descriptor_set_hash = descriptor_set_hash(&catalog);
        Self {
            catalog,
            lsp,
            mcp,
            descriptor_set_hash,
        }
    }
}

/// Sole owner of tool implementation state for one workspace.
pub struct ToolHost {
    workspace_id: String,
    workspace_root: PathBuf,
    snapshot: RwLock<Arc<ToolHostSnapshot>>,
    revision: AtomicU64,
    cache: Arc<ToolCache>,
}

impl std::fmt::Debug for ToolHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolHost")
            .field("workspace_id", &self.workspace_id)
            .field("workspace_root", &self.workspace_root)
            .field("revision", &self.revision())
            .field("cache", &self.cache.stats())
            .finish_non_exhaustive()
    }
}

impl ToolHost {
    #[must_use]
    pub fn new(
        workspace_id: impl Into<String>,
        workspace_root: impl Into<PathBuf>,
        snapshot: ToolHostSnapshot,
    ) -> Self {
        Self {
            workspace_id: workspace_id.into(),
            workspace_root: workspace_root.into(),
            snapshot: RwLock::new(Arc::new(snapshot)),
            revision: AtomicU64::new(1),
            cache: Arc::new(ToolCache::new()),
        }
    }

    #[must_use]
    pub fn builtin(workspace_id: impl Into<String>, workspace_root: impl Into<PathBuf>) -> Self {
        let snapshot = ToolHostSnapshot::new(
            Arc::new(ToolCatalog::builtin()),
            Arc::new(LspRegistry::new()),
            None,
        );
        Self::new(workspace_id, workspace_root, snapshot)
    }

    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    #[must_use]
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    /// Pin catalog, LSP, MCP and cache schema to one coherent request revision.
    #[must_use]
    pub fn pin_snapshot(&self) -> ToolHostLease {
        let guard = self
            .snapshot
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let revision = self.revision();
        let snapshot = guard.clone();
        ToolHostLease {
            workspace_id: self.workspace_id.clone(),
            workspace_root: self.workspace_root.clone(),
            revision,
            snapshot,
            cache: Arc::clone(&self.cache),
        }
    }

    /// Atomically publish a fully built snapshot. Existing requests retain their lease.
    pub fn replace_snapshot(&self, snapshot: ToolHostSnapshot) -> u64 {
        let mut current = self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let descriptor_changed = current.descriptor_set_hash != snapshot.descriptor_set_hash;
        let revision = self.revision.fetch_add(1, Ordering::AcqRel) + 1;
        *current = Arc::new(snapshot);
        drop(current);
        if descriptor_changed {
            self.cache.invalidate_all();
        }
        revision
    }

    #[must_use]
    pub fn cache_stats(&self) -> ToolCacheStats {
        self.cache.stats()
    }

    pub fn invalidate_cache(&self) {
        self.cache.invalidate_all();
    }
}

/// Request-scoped immutable view. Production search and execute APIs require it.
#[derive(Clone)]
pub struct ToolHostLease {
    workspace_id: String,
    workspace_root: PathBuf,
    revision: u64,
    snapshot: Arc<ToolHostSnapshot>,
    cache: Arc<ToolCache>,
}

impl std::fmt::Debug for ToolHostLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolHostLease")
            .field("workspace_id", &self.workspace_id)
            .field("workspace_root", &self.workspace_root)
            .field("revision", &self.revision)
            .field("descriptor_set_hash", &self.snapshot.descriptor_set_hash)
            .finish_non_exhaustive()
    }
}

impl ToolHostLease {
    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    #[must_use]
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn schema_revision(&self) -> u64 {
        u64::from_str_radix(&self.snapshot.descriptor_set_hash, 16).unwrap_or_default()
    }

    #[must_use]
    pub fn snapshot(&self) -> &ToolHostSnapshot {
        &self.snapshot
    }

    #[must_use]
    pub fn cache(&self) -> &ToolCache {
        &self.cache
    }

    #[must_use]
    pub fn search(&self, query: &str, max_results: usize) -> ToolDiscoveryReceipt {
        let query = query.trim().to_string();
        let ids = self.snapshot.catalog.search_ids(&query, max_results);
        let descriptors = ids
            .iter()
            .filter_map(|id| self.snapshot.catalog.descriptor_ref(id))
            .collect();
        ToolDiscoveryReceipt {
            query,
            catalog_revision: self.revision,
            descriptors,
            activation_candidates: ids,
        }
    }

    #[must_use]
    pub fn describe_effect(&self, tool_id: &str, input: &Value) -> ToolEffectDescriptor {
        let catalog_known = self.snapshot.catalog.contains(tool_id);
        let permission = self
            .snapshot
            .catalog
            .required_permission(tool_id)
            .unwrap_or(ToolPermissionMode::DangerFullAccess);
        describe_tool_effect(tool_id, input, permission, catalog_known)
    }

    pub fn execute(
        &self,
        authorization: &ToolExecutionAuthorization,
        tool_id: &str,
        input: &Value,
    ) -> Result<String, ToolHostError> {
        if authorization.tool_id != tool_id {
            return Err(ToolHostError::ToolMismatch {
                authorized: authorization.tool_id.clone(),
                requested: tool_id.to_string(),
            });
        }
        if authorization.permission_lease.trim().is_empty()
            || authorization.timeout_lease.trim().is_empty()
        {
            return Err(ToolHostError::InvalidLease);
        }

        let effective = self.describe_effect(tool_id, input);
        if authorization.descriptor_hash != effective.descriptor_hash {
            return Err(ToolHostError::EffectEscalated {
                authorized_hash: authorization.descriptor_hash.clone(),
                effective_hash: effective.descriptor_hash,
            });
        }
        if !effective.scopes.contains(&authorization.scope) {
            return Err(ToolHostError::ScopeNotAuthorized);
        }
        if effective.idempotency == ToolIdempotency::IdempotentWithKey
            && authorization
                .idempotency_key
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Err(ToolHostError::MissingIdempotencyKey);
        }
        if !self.snapshot.catalog.contains(tool_id) {
            return Err(ToolHostError::ToolNotFound(tool_id.to_string()));
        }

        if crate::mvp_tool_specs()
            .iter()
            .any(|spec| spec.name == tool_id)
        {
            return crate::executor::execute_with_lease(self, tool_id, input)
                .map_err(ToolHostError::Execution);
        }
        if self.snapshot.catalog.has_runtime_tool(tool_id) {
            let (server, tool) = parse_mcp_runtime_id(tool_id)
                .ok_or_else(|| ToolHostError::UnsupportedRuntimeTool(tool_id.to_string()))?;
            let service = self
                .snapshot
                .mcp
                .as_ref()
                .ok_or(ToolHostError::McpUnavailable)?;
            let receipt = service
                .call_tool(mcp::McpToolCallRequest {
                    server: server.to_string(),
                    tool: tool.to_string(),
                    input: input.clone(),
                })
                .map_err(|error| ToolHostError::Execution(error.to_string()))?;
            return serde_json::to_string_pretty(&receipt)
                .map_err(|error| ToolHostError::Execution(error.to_string()));
        }
        self.snapshot
            .catalog
            .execute_plugin(tool_id, input)
            .map_err(ToolHostError::Execution)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ToolHostError {
    #[error("tool `{0}` is not present in the pinned catalog")]
    ToolNotFound(String),
    #[error("authorization is for `{authorized}`, not requested tool `{requested}`")]
    ToolMismatch {
        authorized: String,
        requested: String,
    },
    #[error(
        "tool authorization is stale: effect escalated from {authorized_hash} to {effective_hash}"
    )]
    EffectEscalated {
        authorized_hash: String,
        effective_hash: String,
    },
    #[error("authorization scope does not cover the effective tool scope")]
    ScopeNotAuthorized,
    #[error("authorization permission or timeout lease is empty")]
    InvalidLease,
    #[error("idempotent write authorization is missing its idempotency key")]
    MissingIdempotencyKey,
    #[error("runtime tool `{0}` has no ToolHost implementation adapter")]
    UnsupportedRuntimeTool(String),
    #[error("MCP service is not configured in the pinned ToolHost snapshot")]
    McpUnavailable,
    #[error("tool execution failed: {0}")]
    Execution(String),
}

fn descriptor_set_hash(catalog: &ToolCatalog) -> String {
    let mut definitions = catalog.kernel_definitions(None);
    definitions.sort_by(|left, right| left.name.cmp(&right.name));
    let mut hasher = DefaultHasher::new();
    for definition in definitions {
        definition.name.hash(&mut hasher);
        definition.description.hash(&mut hasher);
        serde_json::to_string(&definition.input_schema)
            .unwrap_or_default()
            .hash(&mut hasher);
        definition.required_permission.as_str().hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}

fn parse_mcp_runtime_id(tool_id: &str) -> Option<(&str, &str)> {
    let mut parts = tool_id.splitn(3, "__");
    match (parts.next(), parts.next(), parts.next()) {
        (Some("mcp"), Some(server), Some(tool)) if !server.is_empty() && !tool.is_empty() => {
            Some((server, tool))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::tool::ToolExecutionAuthorization;
    use serde_json::json;

    #[test]
    fn pinned_lease_keeps_old_snapshot_across_reload() {
        let host = ToolHost::builtin("workspace", "/tmp/workspace");
        let before = host.pin_snapshot();
        let old_hash = before.snapshot().descriptor_set_hash.clone();

        host.replace_snapshot(ToolHostSnapshot::new(
            Arc::new(ToolCatalog::builtin()),
            Arc::new(LspRegistry::new()),
            None,
        ));
        let after = host.pin_snapshot();

        assert_eq!(before.revision(), 1);
        assert_eq!(before.snapshot().descriptor_set_hash, old_hash);
        assert_eq!(after.revision(), 2);
    }

    #[test]
    fn hosts_isolate_cache_and_workspace_identity() {
        let first = ToolHost::builtin("one", "/tmp/one");
        let second = ToolHost::builtin("two", "/tmp/two");
        let first_lease = first.pin_snapshot();
        first_lease.cache().put(
            "one",
            "file:a",
            "read_file",
            "{}",
            first_lease.revision(),
            "a",
        );
        assert!(second
            .pin_snapshot()
            .cache()
            .get("two", "file:a", "read_file", "{}", 1)
            .is_none());
    }

    fn authorization(
        descriptor: &ToolEffectDescriptor,
        idempotency_key: Option<&str>,
    ) -> ToolExecutionAuthorization {
        ToolExecutionAuthorization {
            request_id: "request".to_string(),
            tool_id: descriptor.tool_id.clone(),
            descriptor_hash: descriptor.descriptor_hash.clone(),
            scope: descriptor.scopes[0].clone(),
            permission_lease: "permission-lease".to_string(),
            timeout_lease: "timeout-lease".to_string(),
            idempotency_key: idempotency_key.map(str::to_string),
        }
    }

    #[test]
    fn stale_effect_is_rejected_before_execution() {
        let host = ToolHost::builtin("workspace", "/tmp/workspace");
        let lease = host.pin_snapshot();
        let planned = lease.describe_effect("bash", &json!({"command": "git status"}));
        let error = lease
            .execute(
                &authorization(&planned, None),
                "bash",
                &json!({"command": "rm -rf target"}),
            )
            .expect_err("changed command must invalidate authorization");
        assert!(matches!(error, ToolHostError::EffectEscalated { .. }));
    }

    #[test]
    fn write_requires_idempotency_key() {
        let host = ToolHost::builtin("workspace", "/tmp/workspace");
        let lease = host.pin_snapshot();
        let descriptor = lease.describe_effect(
            "write_file",
            &json!({"path": "/tmp/tool-host-test", "content": "x"}),
        );
        let error = lease
            .execute(
                &authorization(&descriptor, None),
                "write_file",
                &json!({"path": "/tmp/tool-host-test", "content": "x"}),
            )
            .expect_err("write without idempotency key must fail");
        assert_eq!(error, ToolHostError::MissingIdempotencyKey);
    }

    #[test]
    fn authorized_read_executes_against_pinned_host() {
        let path = std::env::temp_dir().join(format!(
            "cowd-tool-host-read-{}-{}.txt",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::write(&path, "pinned-host").unwrap();
        let host = ToolHost::builtin("workspace", std::env::temp_dir());
        let lease = host.pin_snapshot();
        let input = json!({"path": path.to_string_lossy()});
        let descriptor = lease.describe_effect("read_file", &input);
        let output = lease
            .execute(&authorization(&descriptor, None), "read_file", &input)
            .expect("authorized read should execute");
        assert!(output.contains("pinned-host"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn canonical_mcp_runtime_ids_are_parsed_without_guessing() {
        assert_eq!(
            parse_mcp_runtime_id("mcp__filesystem__read__file"),
            Some(("filesystem", "read__file"))
        );
        assert_eq!(parse_mcp_runtime_id("runtime_tool"), None);
    }
}
