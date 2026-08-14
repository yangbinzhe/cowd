//! Workspace-scoped tool implementation host.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use harness_contract::policy::AuthorizationLease;
use harness_contract::tool::{
    GovernedToolInvocation, ResourceAccess, ResourceDemand, ResourceScopeDemand, ToolDependency,
    ToolDiscoveryReceipt, ToolEffectDescriptor, ToolExecutionAuthorization, ToolIdempotency,
    ToolIntent, ToolPermissionMode,
};
use mcp::McpService;
use serde_json::Value;

use crate::lsp_client::LspRegistry;
use crate::path_policy::WorkspacePathPolicy;
use crate::tool_cache::{ToolCache, ToolCacheStats};
use crate::tool_orchestrator::resolve_registered_tool_effect;
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
    authorization_lease_verifier:
        Option<Arc<dyn Fn(&AuthorizationLease) -> bool + Send + Sync + 'static>>,
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
            authorization_lease_verifier: None,
        }
    }

    /// Bind the concrete Runtime lease verifier. Production composition roots
    /// must install this before exposing execution; a missing verifier fails
    /// closed while catalog/search operations remain available.
    #[must_use]
    pub fn with_authorization_lease_verifier(
        mut self,
        verifier: Arc<dyn Fn(&AuthorizationLease) -> bool + Send + Sync + 'static>,
    ) -> Self {
        self.authorization_lease_verifier = Some(verifier);
        self
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
            path_policy: Arc::new(WorkspacePathPolicy::new(&self.workspace_root)),
            revision,
            snapshot,
            cache: Arc::clone(&self.cache),
            authorization_lease_verifier: self.authorization_lease_verifier.clone(),
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
    path_policy: Arc<WorkspacePathPolicy>,
    revision: u64,
    snapshot: Arc<ToolHostSnapshot>,
    cache: Arc<ToolCache>,
    authorization_lease_verifier:
        Option<Arc<dyn Fn(&AuthorizationLease) -> bool + Send + Sync + 'static>>,
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
    pub fn path_policy(&self) -> &WorkspacePathPolicy {
        &self.path_policy
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

    /// Metadata for the complete catalog pinned by this lease. Runtime uses
    /// this to plan its bootstrap schema; it does not make every descriptor
    /// model-visible until a discovery activation accepts it.
    #[must_use]
    pub fn catalog_receipt(&self) -> ToolDiscoveryReceipt {
        let descriptors = self
            .snapshot
            .catalog
            .definitions(None)
            .into_iter()
            .filter_map(|definition| self.snapshot.catalog.descriptor_ref(&definition.name))
            .collect::<Vec<_>>();
        let activation_candidates = descriptors
            .iter()
            .map(|descriptor| descriptor.canonical_id.clone())
            .collect();
        ToolDiscoveryReceipt {
            query: "catalog".to_string(),
            catalog_revision: self.revision,
            descriptors,
            activation_candidates,
        }
    }

    #[must_use]
    pub fn describe_effect(&self, tool_id: &str, input: &Value) -> ToolEffectDescriptor {
        let canonical_id = self
            .snapshot
            .catalog
            .canonical_name(tool_id)
            .unwrap_or_else(|| tool_id.to_string());
        let permission = self
            .snapshot
            .catalog
            .required_permission(&canonical_id)
            .unwrap_or(ToolPermissionMode::DangerFullAccess);
        let resolver = self.snapshot.catalog.effect_resolver(&canonical_id);
        resolve_registered_tool_effect(&resolver, &canonical_id, input, permission)
    }

    /// Validate model-supplied input against the exact Tool definition pinned
    /// by this request. This boundary runs before policy negotiation so an
    /// invalid call can never wait for approval or consume an execution slot.
    pub fn validate_input(&self, tool_id: &str, input: &Value) -> Result<(), ToolHostError> {
        let canonical_id = self
            .snapshot
            .catalog
            .canonical_name(tool_id)
            .ok_or_else(|| ToolHostError::ToolNotFound(tool_id.to_string()))?;
        let definition = self
            .snapshot
            .catalog
            .definitions(None)
            .into_iter()
            .find(|definition| definition.name == canonical_id)
            .ok_or_else(|| ToolHostError::ToolNotFound(canonical_id.clone()))?;
        validate_schema_value(
            &definition.input_schema,
            &definition.input_schema,
            input,
            "$",
        )
        .map_err(|reason| ToolHostError::InputContract {
            tool: canonical_id,
            reason,
        })
    }

    /// Prepare one immutable governed invocation from the pinned catalog
    /// revision. Runtime consumes this descriptor directly and never re-infers
    /// effect or resource behavior from a tool name.
    #[must_use]
    pub fn prepare_governed_invocation(
        &self,
        invocation_id: impl Into<String>,
        tool_id: &str,
        input: &Value,
        depends_on: &[String],
    ) -> GovernedToolInvocation {
        let invocation_id = invocation_id.into();
        let canonical_id = self
            .snapshot
            .catalog
            .canonical_name(tool_id)
            .unwrap_or_else(|| tool_id.to_string());
        let effect = self.describe_effect(&canonical_id, input);
        let explicit_dependencies = depends_on
            .iter()
            .map(|dependency| ToolDependency {
                invocation_id: invocation_id.clone(),
                depends_on: dependency.clone(),
                reason: "model_explicit_dependency".to_string(),
            })
            .collect();
        GovernedToolInvocation {
            contract_version: 1,
            invocation_id: invocation_id.clone(),
            intent: ToolIntent {
                invocation_id: invocation_id.clone(),
                tool_name: canonical_id.clone(),
                normalized_input: canonicalize_json(input),
            },
            resource_demand: resource_demand_for_effect(&effect),
            idempotency_key: format!("{canonical_id}:{invocation_id}:{}", effect.descriptor_hash),
            effect,
            explicit_dependencies,
            compiled_dependencies: Vec::new(),
            catalog_revision: self.revision,
            descriptor_set_hash: self.snapshot.descriptor_set_hash.clone(),
        }
    }

    pub fn execute(
        &self,
        authorization: &ToolExecutionAuthorization,
        tool_id: &str,
        input: &Value,
    ) -> Result<String, ToolHostError> {
        let (canonical_id, value) =
            self.authorize_and_canonicalize(authorization, tool_id, input)?;
        self.dispatch_sync(&canonical_id, value)
    }

    /// Unified asynchronous entry (T4): bash runs on the tokio runtime with
    /// bounded capture and progress samples; every other tool falls back to
    /// the validated blocking path inside `spawn_blocking`.
    pub async fn execute_async(
        &self,
        authorization: &ToolExecutionAuthorization,
        tool_id: &str,
        input: &Value,
    ) -> Result<String, ToolHostError> {
        self.execute_async_with_progress(authorization, tool_id, input, None)
            .await
    }

    pub async fn execute_async_with_progress(
        &self,
        authorization: &ToolExecutionAuthorization,
        tool_id: &str,
        input: &Value,
        progress: Option<Arc<dyn Fn(crate::bash::BashProgressSample) + Send + Sync>>,
    ) -> Result<String, ToolHostError> {
        let (canonical_id, value) =
            self.authorize_and_canonicalize(authorization, tool_id, input)?;
        if canonical_id == "bash" {
            let bash_input: crate::bash::BashCommandInput = serde_json::from_value(value.clone())
                .map_err(|error| {
                ToolHostError::Execution(format!("invalid bash input: {error}"))
            })?;
            let output = crate::bash::execute_bash_async_in_workspace(
                bash_input,
                self.workspace_root(),
                progress,
            )
            .await
            .map_err(|error| ToolHostError::Execution(error.to_string()))?;
            return serde_json::to_string_pretty(&output)
                .map_err(|error| ToolHostError::Execution(error.to_string()));
        }
        let lease = self.clone();
        let value = value.clone();
        tokio::task::spawn_blocking(move || lease.dispatch_sync(&canonical_id, &value))
            .await
            .map_err(|error| ToolHostError::Execution(error.to_string()))?
    }

    /// Validate an authorization at the concrete ToolHost boundary without
    /// dispatching it. Gateway-owned control/MCP/Lark adapters use this before
    /// their own execution implementation so their side effects cannot bypass
    /// the same signed descriptor/scope/revision gate as ordinary tools.
    pub fn validate_authorization(
        &self,
        authorization: &ToolExecutionAuthorization,
        tool_id: &str,
        input: &Value,
    ) -> Result<String, ToolHostError> {
        self.authorize_and_canonicalize(authorization, tool_id, input)
            .map(|(canonical, _)| canonical)
    }

    fn authorize_and_canonicalize<'a>(
        &'a self,
        authorization: &ToolExecutionAuthorization,
        tool_id: &str,
        input: &'a Value,
    ) -> Result<(String, &'a Value), ToolHostError> {
        let canonical_id = self
            .snapshot
            .catalog
            .canonical_name(tool_id)
            .unwrap_or_else(|| tool_id.to_string());
        if authorization.tool_id != canonical_id {
            return Err(ToolHostError::ToolMismatch {
                authorized: authorization.tool_id.clone(),
                requested: canonical_id,
            });
        }
        let effective = self.describe_effect(&canonical_id, input);
        let lease = &authorization.authorization_lease;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        if lease.signature.trim().is_empty() {
            return Err(ToolHostError::MissingLeaseSignature);
        }
        if self
            .authorization_lease_verifier
            .as_ref()
            .is_none_or(|verifier| !verifier(lease))
        {
            return Err(ToolHostError::InvalidLeaseSignature);
        }
        if authorization.policy_revision == 0
            || lease.policy_revision == 0
            || authorization.policy_revision != lease.policy_revision
        {
            return Err(ToolHostError::PolicyRevisionMismatch {
                authorization_revision: authorization.policy_revision,
                lease_revision: lease.policy_revision,
            });
        }
        if lease.effect_descriptor_hash != authorization.descriptor_hash {
            return Err(ToolHostError::LeaseEffectMismatch);
        }
        if authorization.timeout_lease.trim().is_empty() {
            return Err(ToolHostError::MissingTimeoutLease);
        }
        if !lease.is_active_at(now_ms) {
            return Err(ToolHostError::InactiveLease {
                status: format!("{:?}", lease.status).to_ascii_lowercase(),
                remaining_uses: lease.remaining_uses,
                expires_at_ms: lease.expires_at_ms,
                observed_at_ms: now_ms,
            });
        }
        if !lease.permits(&canonical_id, effective.required_permission) {
            return Err(ToolHostError::LeasePermissionDenied {
                authorized: lease.ceiling,
                required: effective.required_permission,
            });
        }

        if authorization.descriptor_hash != effective.descriptor_hash {
            return Err(ToolHostError::EffectEscalated {
                authorized_hash: authorization.descriptor_hash.clone(),
                effective_hash: effective.descriptor_hash,
            });
        }
        if !effective.scopes.contains(&authorization.scope) {
            return Err(ToolHostError::ScopeNotAuthorized);
        }
        if !effective
            .scopes
            .iter()
            .all(|scope| lease.scopes.contains(scope))
        {
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
        if authorization
            .idempotency_key
            .as_deref()
            .is_some_and(|key| !lease.idempotency_key.is_empty() && lease.idempotency_key != key)
        {
            return Err(ToolHostError::LeaseIdempotencyMismatch);
        }
        if !self.snapshot.catalog.contains(&canonical_id) {
            return Err(ToolHostError::ToolNotFound(canonical_id));
        }
        Ok((canonical_id, input))
    }

    fn dispatch_sync(&self, canonical_id: &str, input: &Value) -> Result<String, ToolHostError> {
        if crate::mvp_tool_specs()
            .iter()
            .any(|spec| spec.name == canonical_id)
        {
            return crate::executor::execute_with_lease(self, canonical_id, input)
                .map_err(ToolHostError::Execution);
        }
        if self.snapshot.catalog.has_runtime_tool(canonical_id) {
            let (server, tool) = parse_mcp_runtime_id(canonical_id)
                .ok_or_else(|| ToolHostError::UnsupportedRuntimeTool(canonical_id.to_string()))?;
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
            .execute_plugin(canonical_id, input)
            .map_err(ToolHostError::Execution)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ToolHostError {
    #[error("tool `{0}` is not present in the pinned catalog")]
    ToolNotFound(String),
    #[error("tool `{tool}` input violates its pinned schema: {reason}")]
    InputContract { tool: String, reason: String },
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
    #[error("authorization lease signature is empty")]
    MissingLeaseSignature,
    #[error("authorization lease signature is invalid or no verifier is configured")]
    InvalidLeaseSignature,
    #[error(
        "authorization policy revision mismatch: authorization={authorization_revision}, lease={lease_revision}"
    )]
    PolicyRevisionMismatch {
        authorization_revision: u64,
        lease_revision: u64,
    },
    #[error("authorization lease effect descriptor does not match the authorization")]
    LeaseEffectMismatch,
    #[error("authorization timeout lease is empty")]
    MissingTimeoutLease,
    #[error(
        "authorization lease is not active: status={status}, remaining_uses={remaining_uses}, expires_at_ms={expires_at_ms}, observed_at_ms={observed_at_ms}"
    )]
    InactiveLease {
        status: String,
        remaining_uses: u32,
        expires_at_ms: u64,
        observed_at_ms: u64,
    },
    #[error(
        "authorization lease permission is insufficient: authorized={authorized:?}, required={required:?}"
    )]
    LeasePermissionDenied {
        authorized: harness_contract::policy::PermissionMode,
        required: harness_contract::policy::PermissionMode,
    },
    #[error("authorization lease idempotency key does not match the invocation")]
    LeaseIdempotencyMismatch,
    #[error("idempotent write authorization is missing its idempotency key")]
    MissingIdempotencyKey,
    #[error("runtime tool `{0}` has no ToolHost implementation adapter")]
    UnsupportedRuntimeTool(String),
    #[error("MCP service is not configured in the pinned ToolHost snapshot")]
    McpUnavailable,
    #[error("tool execution failed: {0}")]
    Execution(String),
}

fn validate_schema_value(
    root: &Value,
    schema: &Value,
    value: &Value,
    path: &str,
) -> Result<(), String> {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let pointer = reference
            .strip_prefix('#')
            .ok_or_else(|| format!("{path}: external schema reference is unsupported"))?;
        let target = root
            .pointer(pointer)
            .ok_or_else(|| format!("{path}: unresolved schema reference `{reference}`"))?;
        return validate_schema_value(root, target, value, path);
    }

    if let Some(branches) = schema.get("allOf").and_then(Value::as_array) {
        for branch in branches {
            validate_schema_value(root, branch, value, path)?;
        }
    }
    for keyword in ["anyOf", "oneOf"] {
        if let Some(branches) = schema.get(keyword).and_then(Value::as_array) {
            let matches = branches
                .iter()
                .filter(|branch| validate_schema_value(root, branch, value, path).is_ok())
                .count();
            let valid = if keyword == "oneOf" {
                matches == 1
            } else {
                matches >= 1
            };
            if !valid {
                return Err(format!("{path}: value does not satisfy `{keyword}`"));
            }
        }
    }

    if let Some(expected) = schema.get("const") {
        if value != expected {
            return Err(format!(
                "{path}: value does not match the required constant"
            ));
        }
    }
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array) {
        if !allowed.contains(value) {
            return Err(format!("{path}: value is not in the allowed enum"));
        }
    }
    if let Some(expected) = schema.get("type") {
        let type_matches = match expected {
            Value::String(expected) => schema_type_matches(expected, value),
            Value::Array(expected) => expected
                .iter()
                .filter_map(Value::as_str)
                .any(|expected| schema_type_matches(expected, value)),
            _ => true,
        };
        if !type_matches {
            return Err(format!("{path}: value has the wrong JSON type"));
        }
    }

    if let Some(object) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for field in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(field) {
                    return Err(format!("{path}: missing required field `{field}`"));
                }
            }
        }
        let properties = schema.get("properties").and_then(Value::as_object);
        for (field, field_value) in object {
            if let Some(field_schema) = properties.and_then(|properties| properties.get(field)) {
                validate_schema_value(root, field_schema, field_value, &format!("{path}.{field}"))?;
            } else if schema.get("additionalProperties") == Some(&Value::Bool(false)) {
                return Err(format!("{path}: unexpected field `{field}`"));
            } else if let Some(additional_schema) = schema
                .get("additionalProperties")
                .filter(|additional| additional.is_object())
            {
                validate_schema_value(
                    root,
                    additional_schema,
                    field_value,
                    &format!("{path}.{field}"),
                )?;
            }
        }
    }

    if let Some(array) = value.as_array() {
        validate_count_bounds(schema, array.len(), path, "Items")?;
        if let Some(item_schema) = schema.get("items") {
            for (index, item) in array.iter().enumerate() {
                validate_schema_value(root, item_schema, item, &format!("{path}[{index}]"))?;
            }
        }
    }
    if let Some(text) = value.as_str() {
        validate_count_bounds(schema, text.chars().count(), path, "Length")?;
        if let Some(pattern) = schema.get("pattern").and_then(Value::as_str) {
            let pattern = regex::Regex::new(pattern)
                .map_err(|error| format!("{path}: invalid schema pattern: {error}"))?;
            if !pattern.is_match(text) {
                return Err(format!(
                    "{path}: string does not match the required pattern"
                ));
            }
        }
    }
    if let Some(number) = value.as_f64() {
        if schema
            .get("minimum")
            .and_then(Value::as_f64)
            .is_some_and(|minimum| number < minimum)
        {
            return Err(format!("{path}: number is below the minimum"));
        }
        if schema
            .get("maximum")
            .and_then(Value::as_f64)
            .is_some_and(|maximum| number > maximum)
        {
            return Err(format!("{path}: number exceeds the maximum"));
        }
    }
    Ok(())
}

fn schema_type_matches(expected: &str, value: &Value) -> bool {
    match expected {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "number" => value.is_number(),
        "integer" => value
            .as_number()
            .is_some_and(|number| number.is_i64() || number.is_u64()),
        "string" => value.is_string(),
        _ => true,
    }
}

fn validate_count_bounds(
    schema: &Value,
    count: usize,
    path: &str,
    suffix: &str,
) -> Result<(), String> {
    let minimum_key = format!("min{suffix}");
    if schema
        .get(&minimum_key)
        .and_then(Value::as_u64)
        .is_some_and(|minimum| count < minimum as usize)
    {
        return Err(format!("{path}: value is shorter than `{minimum_key}`"));
    }
    let maximum_key = format!("max{suffix}");
    if schema
        .get(&maximum_key)
        .and_then(Value::as_u64)
        .is_some_and(|maximum| count > maximum as usize)
    {
        return Err(format!("{path}: value exceeds `{maximum_key}`"));
    }
    Ok(())
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
        let resolver = catalog.effect_resolver(&definition.name);
        resolver.resolver_id.hash(&mut hasher);
        resolver.resolver_version.hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}

fn resource_demand_for_effect(effect: &ToolEffectDescriptor) -> ResourceDemand {
    let mut scopes = effect
        .scopes
        .iter()
        .filter_map(|scope| {
            scope.target.clone().map(|key| ResourceScopeDemand {
                key,
                access: if scope.operation == harness_contract::policy::PermissionOperation::Read {
                    ResourceAccess::Read
                } else {
                    ResourceAccess::Write
                },
            })
        })
        .collect::<Vec<_>>();
    scopes.sort_by(|left, right| left.key.cmp(&right.key));
    scopes.dedup();
    ResourceDemand {
        tool_slots: 1,
        process_slots: u32::from(effect.spawns_process),
        network_slots: u32::from(effect.uses_network),
        cpu_weight: if effect.spawns_process { 2 } else { 1 },
        memory_bytes: 0,
        scopes,
    }
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            let mut normalized = serde_json::Map::new();
            for key in keys {
                normalized.insert(key.clone(), canonicalize_json(&map[key]));
            }
            Value::Object(normalized)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_json).collect()),
        _ => value.clone(),
    }
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

    fn test_host(workspace_id: &str, root: impl Into<PathBuf>) -> ToolHost {
        ToolHost::builtin(workspace_id, root).with_authorization_lease_verifier(Arc::new(|lease| {
            lease.signature == "test-signature"
        }))
    }
    use harness_contract::tool::ToolExecutionAuthorization;
    use serde_json::json;

    #[test]
    fn pinned_lease_keeps_old_snapshot_across_reload() {
        let host = test_host("workspace", "/tmp/workspace");
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
        let first = test_host("one", "/tmp/one");
        let second = test_host("two", "/tmp/two");
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

    #[test]
    fn host_plans_aliases_under_the_canonical_authorization_identity() {
        let lease = test_host("workspace", "/tmp/workspace").pin_snapshot();
        let input = json!({"query": "runtime"});
        let canonical = lease.describe_effect("web_search", &input);
        let alias = lease.describe_effect("web-search", &input);
        let invocation = lease.prepare_governed_invocation("search-1", "web_search", &input, &[]);

        assert_eq!(alias, canonical);
        assert_eq!(invocation.intent.tool_name, "web_search");
        assert_eq!(invocation.effect.tool_id, "web_search");
    }

    #[test]
    fn pinned_catalog_rejects_invalid_input_before_authorization() {
        let lease = test_host("workspace", "/tmp/workspace").pin_snapshot();

        let missing = lease
            .validate_input("bash", &json!({}))
            .expect_err("bash.command is required");
        assert!(missing
            .to_string()
            .contains("missing required field `command`"));

        let extra = lease
            .validate_input("bash", &json!({"command": "pwd", "invented": true}))
            .expect_err("unknown fields must follow additionalProperties=false");
        assert!(extra.to_string().contains("unexpected field `invented`"));

        lease
            .validate_input("bash", &json!({"command": "pwd"}))
            .expect("valid command input");
        lease
            .validate_input("enter_plan_mode", &json!({}))
            .expect("legitimate no-argument tools remain valid");
        lease
            .validate_input("enter_plan_mode", &json!({"invented": true}))
            .expect_err("no-argument tools still reject invented fields");
    }

    fn authorization(
        descriptor: &ToolEffectDescriptor,
        idempotency_key: Option<&str>,
    ) -> ToolExecutionAuthorization {
        let idempotency_key = idempotency_key.map(str::to_string);
        ToolExecutionAuthorization {
            request_id: "request".to_string(),
            tool_id: descriptor.tool_id.clone(),
            descriptor_hash: descriptor.descriptor_hash.clone(),
            policy_revision: 1,
            scope: descriptor.scopes[0].clone(),
            authorization_lease: harness_contract::policy::AuthorizationLease {
                lease_id: "permission-lease".to_string(),
                principal_id: "test".to_string(),
                parent_lease_id: None,
                capability: descriptor.tool_id.clone(),
                scopes: descriptor.scopes.clone(),
                ceiling: descriptor.required_permission,
                issued_at_ms: 0,
                expires_at_ms: u64::MAX,
                max_uses: 1,
                remaining_uses: 1,
                idempotency_key: idempotency_key.clone().unwrap_or_default(),
                policy_revision: 1,
                effect_descriptor_hash: descriptor.descriptor_hash.clone(),
                signature: "test-signature".to_string(),
                status: harness_contract::policy::AuthorizationLeaseStatus::Active,
            },
            timeout_lease: "timeout-lease".to_string(),
            idempotency_key,
        }
    }

    #[test]
    fn stale_effect_is_rejected_before_execution() {
        let host = test_host("workspace", "/tmp/workspace");
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
    fn forged_signature_and_revision_drift_fail_closed_at_host_boundary() {
        let host = test_host("workspace", "/tmp/workspace");
        let lease = host.pin_snapshot();
        let input = json!({"path": "/tmp/tool-host-test"});
        let descriptor = lease.describe_effect("read_file", &input);
        let mut forged = authorization(&descriptor, None);
        forged.authorization_lease.signature = "non-empty-forgery".to_string();
        assert_eq!(
            lease
                .validate_authorization(&forged, "read_file", &input)
                .unwrap_err(),
            ToolHostError::InvalidLeaseSignature
        );

        let mut stale = authorization(&descriptor, None);
        stale.policy_revision = 2;
        assert!(matches!(
            lease
                .validate_authorization(&stale, "read_file", &input)
                .unwrap_err(),
            ToolHostError::PolicyRevisionMismatch { .. }
        ));
    }

    #[test]
    fn execution_without_a_concrete_verifier_fails_closed() {
        let lease = ToolHost::builtin("workspace", "/tmp/workspace").pin_snapshot();
        let input = json!({"path": "/tmp/tool-host-test"});
        let descriptor = lease.describe_effect("read_file", &input);
        assert_eq!(
            lease
                .validate_authorization(&authorization(&descriptor, None), "read_file", &input)
                .unwrap_err(),
            ToolHostError::InvalidLeaseSignature
        );
    }

    #[test]
    fn write_requires_idempotency_key() {
        let host = test_host("workspace", "/tmp/workspace");
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
        let host = test_host("workspace", std::env::temp_dir());
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
