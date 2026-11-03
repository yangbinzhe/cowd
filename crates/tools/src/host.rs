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
use crate::tool_orchestrator::{resolve_registered_tool_effect_resolution, ToolEffectResolution};
use crate::ToolCatalog;

/// Immutable implementation snapshot pinned for one request.
#[derive(Clone)]
pub struct ToolHostSnapshot {
    implementation_revision: String,
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
            implementation_revision: uuid::Uuid::new_v4().to_string(),
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
        drop(guard);
        let path_policy = Arc::new(WorkspacePathPolicy::new(&self.workspace_root));
        let bounded_root = bounded_workspace_directory(path_policy.workspace_root());
        ToolHostLease {
            local_network_trace: None,
            orchestrated_read_trace: None,
            workspace_id: self.workspace_id.clone(),
            workspace_root: self.workspace_root.clone(),
            path_policy,
            bounded_root,
            revision,
            snapshot,
            cache: Arc::clone(&self.cache),
            authorization_lease_verifier: self.authorization_lease_verifier.clone(),
            read_cache_authority_digest: None,
        }
    }

    /// Atomically publish a fully built snapshot. Existing requests retain their lease.
    pub fn replace_snapshot(&self, mut snapshot: ToolHostSnapshot) -> u64 {
        let mut current = self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let descriptor_changed = current.descriptor_set_hash != snapshot.descriptor_set_hash;
        snapshot.implementation_revision = uuid::Uuid::new_v4().to_string();
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

fn bounded_workspace_directory(root: &Path) -> Option<Arc<std::fs::File>> {
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{openat2, Mode, OFlags, ResolveFlags};
        openat2(
            rustix::fs::CWD,
            root,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )
        .ok()
        .map(std::fs::File::from)
        .map(Arc::new)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = root;
        None
    }
}

/// Request-scoped immutable view. Production search and execute APIs require it.
#[derive(Clone)]
pub struct ToolHostLease {
    local_network_trace: Option<Arc<std::sync::Mutex<local_network::LocalNetworkTrace>>>,
    orchestrated_read_trace: Option<Arc<std::sync::Mutex<OrchestratedReadTrace>>>,
    workspace_id: String,
    workspace_root: PathBuf,
    path_policy: Arc<WorkspacePathPolicy>,
    bounded_root: Option<Arc<std::fs::File>>,
    revision: u64,
    snapshot: Arc<ToolHostSnapshot>,
    cache: Arc<ToolCache>,
    read_cache_authority_digest: Option<String>,
    authorization_lease_verifier:
        Option<Arc<dyn Fn(&AuthorizationLease) -> bool + Send + Sync + 'static>>,
}

/// Captured at the actual child executor boundary, never parsed from the
/// model-visible orchestration result. This is an acquisition candidate, not
/// proof of commit or delivery to a model.
#[derive(Clone, PartialEq)]
pub struct OrchestratedRead {
    ordinal: usize,
    step_index: usize,
    step_id: String,
    position: usize,
    parent_request_id: String,
    parent_descriptor_hash: String,
    parent_authorization: ToolExecutionAuthorization,
    input: Value,
    output: String,
    effect: ToolEffectDescriptor,
}

impl std::fmt::Debug for OrchestratedRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrchestratedRead").field("step_index", &self.step_index)
            .field("position", &self.position).finish_non_exhaustive()
    }
}

impl OrchestratedRead {
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }
    pub fn step_index(&self) -> usize {
        self.step_index
    }
    pub fn step_id(&self) -> &str {
        &self.step_id
    }
    pub fn position(&self) -> usize {
        self.position
    }
    pub fn parent_request_id(&self) -> &str {
        &self.parent_request_id
    }
    pub fn parent_descriptor_hash(&self) -> &str {
        &self.parent_descriptor_hash
    }
    pub fn parent_authorization(&self) -> &ToolExecutionAuthorization { &self.parent_authorization }
    pub fn input(&self) -> &Value {
        &self.input
    }
    pub fn output(&self) -> &str {
        &self.output
    }
    pub fn effect(&self) -> &ToolEffectDescriptor {
        &self.effect
    }
}

#[derive(Default)]
struct OrchestratedReadTrace {
    reads: Vec<OrchestratedRead>,
    bytes: usize,
    overflow: bool,
    parent_request_id: String,
    parent_descriptor_hash: String,
    parent_authorization: Option<ToolExecutionAuthorization>,
    authorized_scopes: Vec<harness_contract::policy::PermissionScope>,
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

#[path = "local_network.rs"]
mod local_network;

/// Why one child effect of an orchestrated program could not be resolved.
///
/// `InvalidInput` means the child's own input violates its contract: the call
/// would be refused, and the effects the sibling steps declared are still
/// true. `Unprovable` means the effect itself cannot be proven, so the program
/// may touch anything and no exact scope may be claimed.
#[derive(Debug, Clone)]
struct ChildEffectFailure {
    reason: String,
    unprovable: bool,
}

impl ChildEffectFailure {
    fn unprovable(reason: String) -> Self {
        Self { reason, unprovable: true }
    }

    fn invalid_input(reason: String) -> Self {
        Self { reason, unprovable: false }
    }
}

impl ToolHostLease {
    /// Execute under the original parent authorization while retaining actual
    /// successful child reads in a private, per-invocation trace. A caller must
    /// still bind these candidates to the original transaction and verify
    /// wire delivery before treating any of them as observed evidence.
    pub fn execute_with_orchestrated_read_trace(
        &self,
        authorization: &ToolExecutionAuthorization,
        input: &Value,
    ) -> Result<(String, Option<Vec<OrchestratedRead>>), ToolHostError> {
        if self.orchestrated_read_trace.is_some() {
            return Err(ToolHostError::Execution("nested read trace owner".into()));
        }
        let (canonical, value) =
            self.authorize_and_canonicalize(authorization, "tool_orchestrate", input)?;
        let mut lease = self.for_authorized_read_cache(authorization)?;
        let trace = Arc::new(std::sync::Mutex::new(OrchestratedReadTrace {
            authorized_scopes: authorization.authorization_lease.scopes.clone(),
            parent_request_id: authorization.request_id.clone(),
            parent_descriptor_hash: authorization.descriptor_hash.clone(),
            parent_authorization: Some(authorization.clone()),
            ..OrchestratedReadTrace::default()
        }));
        lease.orchestrated_read_trace = Some(Arc::clone(&trace));
        let output = lease.dispatch_sync(&canonical, value)?;
        let mut trace = trace
            .lock()
            .map_err(|_| ToolHostError::Execution("read trace poisoned".into()))?;
        let reads = (!trace.overflow).then(|| std::mem::take(&mut trace.reads));
        Ok((output, reads))
    }

    pub(crate) fn record_orchestrated_read(
        &self,
        step_index: usize,
        step_id: &str,
        position: usize,
        input: &Value,
        output: &str,
    ) -> Result<(), String> {
        let Some(trace) = &self.orchestrated_read_trace else {
            return Ok(());
        };
        let effect = self.describe_effect("read_file", input);
        let mut trace = trace.lock().map_err(|_| "read trace poisoned")?;
        if effect.scopes.is_empty()
            || !effect
                .scopes
                .iter()
                .all(|scope| trace.authorized_scopes.contains(scope))
        {
            return Ok(());
        }
        let bytes = serde_json::to_vec(input)
            .map_err(|error| error.to_string())?
            .len()
            .saturating_add(output.len());
        if trace.reads.len() >= 400 || trace.bytes.saturating_add(bytes) > 4 * 1024 * 1024 {
            trace.overflow = true;
            trace.reads.clear();
            return Ok(());
        }
        if trace.overflow {
            return Ok(());
        }
        trace.bytes += bytes;
        let ordinal = trace.reads.len() + 1;
        let parent_request_id = trace.parent_request_id.clone();
        let parent_descriptor_hash = trace.parent_descriptor_hash.clone();
        let parent_authorization = trace.parent_authorization.clone().ok_or("read trace parent authorization missing")?;
        trace.reads.push(OrchestratedRead {
            ordinal,
            step_index,
            step_id: step_id.into(),
            position,
            parent_request_id,
            parent_descriptor_hash,
            parent_authorization,
            input: input.clone(),
            output: output.into(),
            effect,
        });
        Ok(())
    }

    pub(crate) fn read_cache_authority_digest(&self) -> Option<&str> {
        self.read_cache_authority_digest.as_deref()
    }

    fn for_authorized_read_cache(
        &self,
        authorization: &ToolExecutionAuthorization,
    ) -> Result<Self, ToolHostError> {
        use sha2::{Digest, Sha256};
        let signed = &authorization.authorization_lease;
        let mut scopes = signed
            .scopes
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ToolHostError::Execution(e.to_string()))?;
        scopes.sort();
        scopes.dedup();
        let identity = serde_json::to_vec(&(
            self.implementation_revision(),
            authorization.policy_revision,
            &authorization.scope,
            &scopes,
            signed.ceiling,
            &signed.capability,
            &authorization.descriptor_hash,
        ))
        .map_err(|e| ToolHostError::Execution(e.to_string()))?;
        let mut lease = self.clone();
        lease.read_cache_authority_digest = Some(format!("{:x}", Sha256::digest(identity)));
        Ok(lease)
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
    pub fn implementation_revision(&self) -> &str {
        &self.snapshot.implementation_revision
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
            .filter_map(|id| self.descriptor_ref(id))
            .collect();
        ToolDiscoveryReceipt {
            query,
            catalog_revision: self.revision,
            descriptors,
            activation_candidates: ids,
        }
    }

    pub fn known_unavailability(&self, tool_id: &str) -> Option<&'static str> {
        (self.snapshot.mcp.is_none()
            && (parse_mcp_runtime_id(tool_id).is_some()
                || matches!(
                    tool_id,
                    "mcp_tool" | "list_mcp_resources_tool" | "read_mcp_resource_tool"
                )))
        .then_some("mcp_not_configured")
    }

    fn descriptor_ref(&self, tool_id: &str) -> Option<harness_contract::tool::ToolDescriptorRef> {
        let mut descriptor = self.snapshot.catalog.descriptor_ref(tool_id)?;
        if self
            .known_unavailability(&descriptor.canonical_id)
            .is_some()
        {
            descriptor.health = harness_contract::tool::ToolDescriptorHealth::Unavailable;
        }
        Some(descriptor)
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
            .filter_map(|definition| self.descriptor_ref(&definition.name))
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
        self.describe_effect_resolution(tool_id, input).descriptor
    }

    #[must_use]
    pub fn describe_effect_resolution(&self, tool_id: &str, input: &Value) -> ToolEffectResolution {
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
        if canonical_id == "lsp" && resolver.resolver_id == "builtin.readonly_process" {
            let roots = input
                .get("path")
                .and_then(Value::as_str)
                .and_then(|path| self.snapshot.lsp.find_server_for_path(path))
                .and_then(|server| server.root_path)
                .and_then(|root| std::path::PathBuf::from(root).canonicalize().ok())
                .map(|root| {
                    let target =
                        if self.workspace_root().canonicalize().ok().as_ref() == Some(&root) {
                            None
                        } else {
                            Some(root.to_string_lossy().into_owned())
                        };
                    vec![target]
                });
            return crate::tool_orchestrator::resolve_lsp_tool_effect_resolution(
                &resolver, input, permission, roots,
            );
        }
        if canonical_id == "tool_orchestrate" {
            let preflight = crate::executor::orchestrate::effect_preflight(input);
            // A child whose effect cannot be proven must not erase the effects
            // the program already declared. Dropping every child on the first
            // failure described a program that also writes as if it declared
            // nothing, which is narrower than what the program may do. The
            // resolved children are kept, the unresolved ones are reported as
            // unresolved, and the composite stays conservative because an
            // unresolved composite is unknown/Secret with the widest
            // permission - so the decision still fails closed.
            let (children, child_indices, unresolved) = match preflight.as_ref() {
                Ok(calls) => self.orchestration_child_effects_partial(calls),
                Err(_) => (Vec::new(), Vec::new(), true),
            };
            let floor = preflight
                .as_ref()
                .map(|calls| {
                    calls.iter().fold(permission, |floor, call| {
                        crate::tool_orchestrator::stricter_permission(
                            floor,
                            self.snapshot
                                .catalog
                                .required_permission(&call.tool)
                                .unwrap_or(ToolPermissionMode::DangerFullAccess),
                        )
                    })
                })
                .unwrap_or(ToolPermissionMode::DangerFullAccess);
            let mut fingerprints = preflight
                .as_ref()
                .ok()
                .into_iter()
                .flatten()
                .zip(child_indices.iter())
                .map(|(call, index)| {
                    let child = &children[*index];
                    format!(
                        "{}:{}:{}:{}",
                        call.step_id,
                        call.operation,
                        call.maximum_calls,
                        child.descriptor.descriptor_hash
                    )
                })
                .collect::<Vec<_>>();
            fingerprints.push(self.snapshot.descriptor_set_hash.clone());
            fingerprints.push(self.workspace_root().to_string_lossy().into_owned());
            return crate::tool_orchestrator::resolve_composite_tool_effect_resolution(
                &resolver,
                input,
                floor,
                &children,
                &fingerprints,
                unresolved,
            );
        }
        resolve_registered_tool_effect_resolution(&resolver, &canonical_id, input, permission)
    }

    pub(crate) fn orchestration_child_effects(
        &self,
        calls: &[crate::executor::orchestrate::PreflightCall],
    ) -> Result<Vec<ToolEffectResolution>, String> {
        calls
            .iter()
            .map(|call| {
                self.orchestration_child_effect(call)
                    .map_err(|failure| failure.reason)
            })
            .collect()
    }

    /// Resolve the child effects that can be proven and report whether any
    /// child could not be resolved, together with the index of each resolved
    /// child so a fingerprint still pairs a step with its own effect.
    pub(crate) fn orchestration_child_effects_partial(
        &self,
        calls: &[crate::executor::orchestrate::PreflightCall],
    ) -> (Vec<ToolEffectResolution>, Vec<usize>, bool) {
        let mut children = Vec::new();
        let mut indices = Vec::new();
        let mut unresolved = false;
        for (index, call) in calls.iter().enumerate() {
            match self.orchestration_child_effect(call) {
                Ok(effect) => {
                    children.push(effect);
                    indices.push(index);
                }
                Err(failure) if failure.unprovable => {
                    // The effect of this child cannot be proven, so the
                    // program may touch anything: no exact scope may be
                    // claimed and every child is dropped.
                    return (Vec::new(), Vec::new(), true);
                }
                Err(_) => unresolved = true,
            }
        }
        (children, indices, unresolved)
    }

    /// Whether this child's own declared file scope is invalid rather than
    /// merely unproven.
    ///
    /// A bounded compute declares the exact files it may read through
    /// `input_files`. A compute step that does not declare them violates that
    /// contract, and the call is refused at execution; the effects the sibling
    /// steps declared are still true, so they stay in the description. Any
    /// other effect that cannot be proven leaves the whole program
    /// unprovable, and then no exact scope may be claimed at all.
    fn child_declares_an_invalid_file_scope(
        &self,
        call: &crate::executor::orchestrate::PreflightCall,
    ) -> bool {
        if call.tool == "execute_code" {
            return crate::sandbox_exec::bounded_compute_input_files(&call.input).is_err();
        }
        crate::executor::orchestrate::original_static_file_scopes(&call.tool, &call.input).is_err()
    }

    fn orchestration_child_effect(
        &self,
        call: &crate::executor::orchestrate::PreflightCall,
    ) -> Result<ToolEffectResolution, ChildEffectFailure> {
        {
            if !crate::executor::is_allowed_orchestration_tool(&call.tool)
                || !self.snapshot.catalog.contains(&call.tool)
            {
                return Err(ChildEffectFailure::unprovable(format!(
                    "tool_orchestrate cannot dispatch pinned child `{}`",
                    call.tool
                )));
            }
            let resolver = self.snapshot.catalog.effect_resolver(&call.tool);
            let permission = self
                .snapshot
                .catalog
                .required_permission(&call.tool)
                .ok_or_else(|| {
                    ChildEffectFailure::unprovable(format!(
                        "missing pinned permission for `{}`",
                        call.tool
                    ))
                })?;
            let effect = if call.references.is_empty() {
                self.describe_effect_resolution(&call.tool, &call.input)
            } else {
                crate::tool_orchestrator::resolve_orchestration_child_effect_resolution(
                    &resolver,
                    &call.tool,
                    &call.input,
                    permission,
                    &call.references,
                )
                .map_err(|reason| {
                    if self.child_declares_an_invalid_file_scope(call) {
                        ChildEffectFailure::invalid_input(reason)
                    } else {
                        ChildEffectFailure::unprovable(reason)
                    }
                })?
            };
            if effect.descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Unknown
            {
                // Two different failures must not be described the same way.
                // A child whose OWN input violates its contract (a compute
                // step without a declared file set, for example) is an invalid
                // call: the program is refused at execution, and the effects
                // the sibling steps declared are still true, so they stay in
                // the description. A child whose effect simply cannot be
                // proven (a dynamic input) leaves the whole program
                // unprovable, and then no exact scope may be claimed at all.
                let invalid_input = self.child_declares_an_invalid_file_scope(call);
                let reason = format!(
                    "tool_orchestrate cannot prove complete effects for `{}` at {} ({})",
                    call.tool, call.step_id, call.operation
                );
                return Err(if invalid_input {
                    ChildEffectFailure::invalid_input(reason)
                } else {
                    ChildEffectFailure::unprovable(reason)
                });
            }
            Ok(effect)
        }
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
            .definition(&canonical_id)
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
            origin: None,
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
        self.for_authorized_read_cache(authorization)?
            .dispatch_sync(&canonical_id, value)
    }

    pub fn prepare_bounded_process(
        &self,
        authorization: &ToolExecutionAuthorization,
        input: &Value,
        binding: harness_contract::context::ToolExecutionBinding,
    ) -> Result<crate::PreparedToolExecution, ToolHostError> {
        self.validate_input("execute_code", input)?;
        self.validate_authorization(authorization, "execute_code", input)?;
        if self
            .workspace_root()
            .canonicalize()
            .map_err(|error| ToolHostError::Execution(error.to_string()))?
            != self.path_policy.workspace_root()
        {
            return Err(ToolHostError::Execution(
                "bounded_original_workspace_changed".into(),
            ));
        }
        let root = self.bounded_root.as_ref().ok_or_else(|| {
            ToolHostError::Execution("bounded_original_workspace_unavailable".into())
        })?;
        let descriptor = self.describe_effect("execute_code", input);
        let descriptor_value = serde_json::to_value(&descriptor)
            .map_err(|error| ToolHostError::Execution(error.to_string()))?;
        let descriptor_digest = harness_contract::step::canonical_digest(
            harness_contract::context::TOOL_EXECUTION_DESCRIPTOR_DOMAIN,
            &descriptor_value,
        )
        .map_err(|error| ToolHostError::Execution(error.to_string()))?;
        let descriptor_sha256 = descriptor_digest
            .strip_prefix("sha256:")
            .ok_or_else(|| ToolHostError::Execution("bounded_descriptor_digest_invalid".into()))?;
        if binding.descriptor_sha256 != descriptor_sha256
            || authorization
                .idempotency_key
                .as_deref()
                .is_some_and(|key| key != binding.call_key)
        {
            return Err(ToolHostError::Execution(
                "bounded_process_binding_mismatch".into(),
            ));
        }
        let seconds = authorization
            .timeout_lease
            .strip_prefix("timeout:")
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                ToolHostError::Execution("bounded_authorization_timeout_invalid".into())
            })?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| ToolHostError::Execution(error.to_string()))?
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let remaining = authorization
            .authorization_lease
            .expires_at_ms
            .saturating_sub(now);
        let ceiling = std::time::Duration::from_millis(
            seconds.saturating_mul(1000).min(remaining).min(120000),
        );
        if ceiling.is_zero() {
            return Err(ToolHostError::Execution(
                "bounded_authorization_expired".into(),
            ));
        }
        let deadline = std::time::Instant::now() + ceiling;
        crate::sandbox_exec::prepare(
            self.path_policy.workspace_root(),
            input,
            binding,
            Some(root.as_ref()),
        )
        .map(|prepared| prepared.restrict_deadline(deadline))
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
            // `shell` is a backend selector, not part of the bash/powershell
            // input contract (both deny unknown fields), so it is stripped
            // before parsing. PowerShell has no async streaming executor, so
            // it is routed through the sync dispatcher; `auto`/`bash`/absent
            // fall through to the async bash executor.
            let shell = value
                .get("shell")
                .and_then(Value::as_str)
                .map(str::to_string);
            if shell.as_deref() == Some("powershell") {
                let lease = self.for_authorized_read_cache(authorization)?;
                let canonical = canonical_id.clone();
                let value = value.clone();
                return tokio::task::spawn_blocking(move || {
                    lease.dispatch_sync(&canonical, &value)
                })
                .await
                .map_err(|error| ToolHostError::Execution(error.to_string()))?;
            }
            let mut bash_value = value.clone();
            if shell.is_some() {
                if let Some(object) = bash_value.as_object_mut() {
                    object.remove("shell");
                }
            }
            let bash_input: crate::bash::BashCommandInput = serde_json::from_value(bash_value)
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
        let lease = self.for_authorized_read_cache(authorization)?;
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
        if canonical_id == "tool_orchestrate" {
            crate::executor::orchestrate::effect_preflight(input)
                .and_then(|calls| self.orchestration_child_effects(&calls))
                .map_err(|reason| ToolHostError::InputContract {
                    tool: canonical_id.clone(),
                    reason,
                })?;
        }
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
            return Err(format!(
                "{path}: value has the wrong JSON type{}",
                declared_type_guidance(expected, value)
            ));
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

/// The JSON type name of a value, for a refusal the model can act on.
fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(number) => {
            if number.is_f64() && !number.is_i64() && !number.is_u64() {
                "number"
            } else {
                "integer"
            }
        }
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Name what the schema wants and what actually arrived.
///
/// `value has the wrong JSON type` says where the call is wrong and nothing
/// about what to send instead, so a model that quoted an array field as a
/// string re-emits the identical frame. One live run did exactly that twice,
/// spent its single frame-repair budget on it, and ended the objective
/// `partial` with three Teams and zero Tasks. Everything here is derived from
/// the schema and the value: no tool-specific branch, no invented shape.
fn declared_type_guidance(expected: &Value, value: &Value) -> String {
    let declared = match expected {
        Value::String(expected) => expected.clone(),
        Value::Array(expected) => expected
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" or "),
        _ => return String::new(),
    };
    let received = json_type_name(value);
    let quoted_container = value.is_string()
        && declared.split(" or ").any(|kind| {
            matches!(
                kind,
                "array" | "object" | "integer" | "number" | "boolean" | "null"
            )
        });
    format!(
        " (declared type: {declared}; received: {received}; send a {declared} value{})",
        if quoted_container {
            format!(" as raw JSON, not as a JSON-encoded string")
        } else {
            String::new()
        }
    )
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
            scope
                .target
                .clone()
                .or_else(|| {
                    (scope.resource == harness_contract::policy::PermissionResource::File)
                        .then(|| ".".to_string())
                })
                .map(|key| ResourceScopeDemand {
                    key,
                    access: if scope.operation
                        == harness_contract::policy::PermissionOperation::Read
                    {
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
    fn pinned_lease_keeps_old_schema_and_resolver_while_new_lease_gets_new_generation() {
        use crate::permissions::PermissionMode;
        use harness_contract::tool::ToolEffectResolverSpec;

        let runtime_tool = |schema: Value, resolver_version: u32| crate::RuntimeToolDefinition {
            name: "generation_probe_tool".into(),
            description: Some("pinned generation probe".into()),
            input_schema: schema,
            required_permission: PermissionMode::ReadOnly,
            effect_resolver: ToolEffectResolverSpec {
                resolver_id: "runtime.readonly".into(),
                resolver_version,
            },
        };
        let old_schema = json!({
            "type": "object",
            "properties": {"old_field": {"type": "string"}},
            "required": ["old_field"],
        });
        let new_schema = json!({
            "type": "object",
            "properties": {"new_field": {"type": "string"}},
            "required": ["new_field"],
        });
        let old_catalog = ToolCatalog::builtin()
            .with_runtime_tools(vec![runtime_tool(old_schema.clone(), 1)])
            .unwrap();
        let new_catalog = ToolCatalog::builtin()
            .with_runtime_tools(vec![runtime_tool(new_schema.clone(), 2)])
            .unwrap();
        let host = ToolHost::new(
            "generation-workspace",
            "/tmp/generation-workspace",
            ToolHostSnapshot::new(Arc::new(old_catalog), Arc::new(LspRegistry::new()), None),
        )
        .with_authorization_lease_verifier(Arc::new(|_| true));
        let old_lease = host.pin_snapshot();

        host.replace_snapshot(ToolHostSnapshot::new(
            Arc::new(new_catalog),
            Arc::new(LspRegistry::new()),
            None,
        ));
        let new_lease = host.pin_snapshot();

        assert_eq!(
            old_lease
                .snapshot()
                .catalog
                .definition("generation_probe_tool")
                .unwrap()
                .input_schema,
            old_schema
        );
        assert_eq!(
            new_lease
                .snapshot()
                .catalog
                .definition("generation_probe_tool")
                .unwrap()
                .input_schema,
            new_schema
        );
        assert_eq!(
            old_lease
                .snapshot()
                .catalog
                .effect_resolver("generation_probe_tool")
                .resolver_version,
            1
        );
        assert_eq!(
            new_lease
                .snapshot()
                .catalog
                .effect_resolver("generation_probe_tool")
                .resolver_version,
            2
        );
        assert_ne!(
            old_lease.snapshot().descriptor_set_hash,
            new_lease.snapshot().descriptor_set_hash
        );
        old_lease
            .validate_input("generation_probe_tool", &json!({"old_field": "yes"}))
            .unwrap();
        assert!(old_lease
            .validate_input("generation_probe_tool", &json!({"new_field": "no"}))
            .is_err());
        new_lease
            .validate_input("generation_probe_tool", &json!({"new_field": "yes"}))
            .unwrap();
        assert!(new_lease
            .validate_input("generation_probe_tool", &json!({"old_field": "no"}))
            .is_err());
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
            .validate_input("plan_mode", &json!({"action": "enter"}))
            .expect("legitimate no-argument tools remain valid");
        lease
            .validate_input("plan_mode", &json!({"action": "enter", "invented": true}))
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
    fn orchestrated_trace_records_actual_successes_under_parent_grant() {
        let root = std::env::temp_dir().join(format!("cowd-child-read-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("a.txt"), "alpha\n").unwrap();
        std::fs::write(root.join("b.txt"), "beta\n").unwrap();
        let lease = test_host("child-read", &root).pin_snapshot();
        let input = json!({"steps":[{"id":"reads","op":"batch","on_error":"collect","calls":[
            {"tool":"read_file","input":{"path":"a.txt"},"as":"a"},
            {"tool":"read_file","input":{"path":"b.txt"},"as":"b"},
            {"tool":"read_file","input":{"path":"missing.txt"},"as":"missing"}
        ]}]});
        let effect = lease.describe_effect("tool_orchestrate", &input);
        let auth = authorization(&effect, None);
        let (output, reads) = lease
            .execute_with_orchestrated_read_trace(&auth, &input)
            .unwrap();
        let reads = reads.unwrap();
        assert_eq!(reads.len(), 2);
        assert_eq!(
            reads
                .iter()
                .map(|read| read.ordinal)
                .collect::<std::collections::BTreeSet<_>>(),
            [1, 2].into_iter().collect()
        );
        for read in &reads {
            let path = read.input["path"].as_str().unwrap();
            assert!(matches!(path, "a.txt" | "b.txt"));
            assert_eq!(read.step_id, "reads");
            assert_eq!(read.step_index, 0);
            assert_eq!(read.parent_request_id(), auth.request_id);
            assert_eq!(read.parent_descriptor_hash(), auth.descriptor_hash);
            assert_eq!(read.position, if path == "a.txt" { 0 } else { 1 });
            assert_eq!(read.effect, lease.describe_effect("read_file", &read.input));
            assert!(read
                .effect
                .scopes
                .iter()
                .all(|scope| auth.authorization_lease.scopes.contains(scope)));
            let parsed: Value = serde_json::from_str(&read.output).unwrap();
            assert_eq!(parsed["type"], "text");
            assert!(output.contains(parsed["file"]["content"].as_str().unwrap().trim()));
        }
        let mut invalid = auth.clone();
        invalid.authorization_lease.scopes.clear();
        assert!(lease
            .execute_with_orchestrated_read_trace(&invalid, &input)
            .is_err());
        let (_, next) = lease
            .execute_with_orchestrated_read_trace(&auth, &input)
            .unwrap();
        assert_eq!(next.unwrap().len(), 2, "traces are invocation-local");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn orchestrated_trace_duplicate_step_names_have_distinct_positions() {
        let root = std::env::temp_dir().join(format!("cowd-child-steps-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("a.txt"), "alpha\n").unwrap();
        let lease = test_host("child-steps", &root).pin_snapshot();
        let input = json!({"steps":[
            {"id":"same","op":"call","tool":"read_file","input":{"path":"a.txt"}},
            {"id":"same","op":"call","tool":"read_file","input":{"path":"a.txt"}}
        ]});
        let auth = authorization(&lease.describe_effect("tool_orchestrate", &input), None);
        let (_, reads) = lease
            .execute_with_orchestrated_read_trace(&auth, &input)
            .unwrap();
        let reads = reads.unwrap();
        assert_eq!(reads.len(), 2);
        assert_eq!(
            reads
                .iter()
                .map(|read| (read.step_index, read.position))
                .collect::<Vec<_>>(),
            [(0, 0), (1, 0)]
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn orchestrated_trace_overflow_discards_all_candidates() {
        let root = std::env::temp_dir();
        let mut lease = test_host("trace-bound", &root).pin_snapshot();
        let input = json!({"path":"a.txt"});
        let effect = lease.describe_effect("read_file", &input);
        let trace = Arc::new(std::sync::Mutex::new(OrchestratedReadTrace {
            authorized_scopes: effect.scopes.clone(),
            parent_authorization: Some(authorization(&effect, None)),
            ..OrchestratedReadTrace::default()
        }));
        lease.orchestrated_read_trace = Some(Arc::clone(&trace));
        lease
            .record_orchestrated_read(0, "reads", 0, &input, "small")
            .unwrap();
        lease
            .record_orchestrated_read(0, "reads", 1, &input, &"x".repeat(4 * 1024 * 1024))
            .unwrap();
        lease
            .record_orchestrated_read(0, "reads", 2, &input, "later")
            .unwrap();
        let trace = trace.lock().unwrap();
        assert!(trace.overflow);
        assert!(trace.reads.is_empty());
    }

    #[test]
    fn bounded_host_preserves_original_unknown_grant_and_intersects_timeout() {
        use harness_contract::context::{
            ArtifactRef, ToolExecutionBinding, ToolExecutionStopReason, ToolExecutionTerminal,
        };
        use sha2::{Digest, Sha256};
        let root = std::env::temp_dir().join(format!("cowd-bounded-host-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let lease = test_host("bounded-host", &root).pin_snapshot();
        let input = json!({"language":"python","code":"import time; time.sleep(600)","input_files":[],"timeout_ms":120000});
        let effect = lease.describe_effect("execute_code", &input);
        assert_eq!(
            effect.effect_kind,
            harness_contract::tool::ToolEffectKind::Process
        );
        assert_eq!(effect.idempotency, ToolIdempotency::Unknown);
        let mut grant = authorization(&effect, None);
        grant.timeout_lease = "timeout:1".into();
        let binding = ToolExecutionBinding {
            call_key: "original-process-call".into(),
            tool_use_id: "original-process-tool".into(),
            input_sha256: harness_contract::step::canonical_digest(
                harness_contract::context::TOOL_EXECUTION_INPUT_DOMAIN,
                &input,
            )
            .unwrap()
            .strip_prefix("sha256:")
            .unwrap()
            .to_owned(),
            descriptor_sha256: harness_contract::step::canonical_digest(
                harness_contract::context::TOOL_EXECUTION_DESCRIPTOR_DOMAIN,
                &serde_json::to_value(&effect).unwrap(),
            )
            .unwrap()
            .strip_prefix("sha256:")
            .unwrap()
            .to_owned(),
            authority_sha256: "a".repeat(64),
        };
        assert_eq!(effect.descriptor_hash.len(), 16);
        assert_eq!(binding.descriptor_sha256.len(), 64);
        let mut old_binding = binding.clone();
        old_binding.descriptor_sha256 = effect.descriptor_hash.clone();
        assert!(lease
            .prepare_bounded_process(&grant, &input, old_binding)
            .is_err());
        let mut changed_descriptor = serde_json::to_value(&effect).unwrap();
        changed_descriptor["spawns_process"] = json!(false);
        assert_eq!(
            changed_descriptor["descriptor_hash"],
            effect.descriptor_hash
        );
        let mut changed_binding = binding.clone();
        changed_binding.descriptor_sha256 = harness_contract::step::canonical_digest(
            harness_contract::context::TOOL_EXECUTION_DESCRIPTOR_DOMAIN,
            &changed_descriptor,
        )
        .unwrap()
        .strip_prefix("sha256:")
        .unwrap()
        .to_owned();
        assert_ne!(changed_binding.descriptor_sha256, binding.descriptor_sha256);
        assert!(lease
            .prepare_bounded_process(&grant, &input, changed_binding)
            .is_err());
        for index in 0..effect.scopes.len() {
            let mut incomplete = grant.clone();
            incomplete.authorization_lease.scopes.remove(index);
            assert!(lease
                .prepare_bounded_process(&incomplete, &input, binding.clone())
                .is_err());
        }
        let mut missing_timeout = grant.clone();
        missing_timeout.timeout_lease = "invented".into();
        assert!(lease
            .prepare_bounded_process(&missing_timeout, &input, binding.clone())
            .is_err());
        let prepared = lease
            .prepare_bounded_process(&grant, &input, binding.clone())
            .unwrap();
        let source = ArtifactRef::durable(
            "artifact://bounded-host-unit",
            format!(
                "sha256:{:x}",
                Sha256::digest(prepared.source_bundle_bytes())
            ),
            prepared.source_bundle_bytes().len() as u64,
            "application/octet-stream",
            "test",
        );
        let admission = prepared.admission(&source).unwrap();
        assert_eq!(admission.binding, binding);
        let completed = prepared.run(admission, &crate::ToolExecutionCancellation::new());
        assert!(
            matches!(
                completed.terminal(),
                ToolExecutionTerminal::Settled {
                    stop_reason: ToolExecutionStopReason::Timeout,
                    ..
                }
            ),
            "{:?}",
            completed.terminal()
        );
        assert_eq!(
            completed.admission().binding.input_sha256,
            binding.input_sha256
        );
        assert!(grant.idempotency_key.is_none());
        assert_eq!(input["timeout_ms"], 120000);
        let backup = root.with_extension("original");
        std::fs::rename(&root, &backup).unwrap();
        std::fs::create_dir(&root).unwrap();
        assert!(lease
            .prepare_bounded_process(&grant, &input, binding.clone())
            .is_err());
        std::fs::remove_dir(&root).unwrap();
        std::fs::rename(&backup, &root).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn typed_resolution_is_canonical_but_never_shrinks_authorization() {
        use harness_contract::goal::{ProofEffectPurpose, ProofEffectScope};

        let lease = test_host("typed-resolution", "/tmp/typed-resolution").pin_snapshot();
        for (tool, input) in [
            (
                "edit",
                json!({"action":"write","path":"src/a","content":"literal"}),
            ),
            (
                "edit",
                json!({"action":"patch","edits":[{"path":"src/a","old_string":"old","new_string":"new"}]}),
            ),
            (
                "tool_orchestrate",
                json!({"steps":[{"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"src/a","content":"literal"}}]}),
            ),
        ] {
            let resolution = lease.describe_effect_resolution(tool, &input);
            assert_eq!(resolution.descriptor, lease.describe_effect(tool, &input));
            assert_eq!(
                lease
                    .prepare_governed_invocation("typed-call", tool, &input, &[])
                    .effect,
                resolution.descriptor
            );
            let admitted = ProofEffectScope {
                version: 2,
                descriptor_hash: resolution.descriptor.descriptor_hash.clone(),
                complete: false,
                targets: Vec::new(),
                scope_purposes: resolution.scope_purposes.clone(),
            };
            admitted
                .validate_purpose_mapping(&resolution.descriptor)
                .unwrap();
            assert!(admitted
                .scope_purposes
                .iter()
                .any(|entry| entry.purpose == ProofEffectPurpose::AutoCheckpointMaintenance));
            let authorized = authorization(&resolution.descriptor, Some("typed-idempotency"));
            lease
                .validate_authorization(&authorized, tool, &input)
                .unwrap();
            for removed in 0..resolution.descriptor.scopes.len() {
                let mut incomplete = authorized.clone();
                incomplete.authorization_lease.scopes.remove(removed);
                assert_eq!(
                    lease
                        .validate_authorization(&incomplete, tool, &input)
                        .unwrap_err(),
                    ToolHostError::ScopeNotAuthorized
                );
            }
            let mut forged = admitted.clone();
            let entry = forged
                .scope_purposes
                .iter_mut()
                .find(|entry| {
                    entry.purpose == ProofEffectPurpose::BusinessEffect
                        && entry.declared_scope.resource
                            == harness_contract::policy::PermissionResource::File
                        && entry.declared_scope.operation
                            == harness_contract::policy::PermissionOperation::Write
                })
                .unwrap();
            entry.purpose = ProofEffectPurpose::AutoCheckpointMaintenance;
            forged
                .validate_purpose_mapping(&resolution.descriptor)
                .unwrap();
            assert_ne!(
                forged.scope_purposes,
                lease
                    .describe_effect_resolution(tool, &input)
                    .scope_purposes
            );
            assert_eq!(
                forged.descriptor_hash,
                resolution.descriptor.descriptor_hash
            );
        }
        let query = json!({"query":"runtime"});
        assert_eq!(
            lease.describe_effect_resolution("web_search", &query),
            lease.describe_effect_resolution("web-search", &query)
        );
        let explicit = lease
            .describe_effect_resolution("checkpoint", &json!({"action":"create","paths":["src"]}));
        assert!(explicit
            .scope_purposes
            .iter()
            .any(|entry| entry.purpose == ProofEffectPurpose::BusinessEffect));
        assert!(!explicit
            .scope_purposes
            .iter()
            .any(|entry| entry.purpose == ProofEffectPurpose::AutoCheckpointMaintenance));
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

    /// A refusal has to say what to send, not only where the call is wrong.
    ///
    /// `value has the wrong JSON type` named the field and nothing else, and a
    /// model answered it by re-emitting the identical frame - twice, which
    /// spent its single frame-repair budget and ended the objective `partial`
    /// with three Teams and zero Tasks.
    #[test]
    fn a_type_mismatch_refusal_names_the_declared_type_and_the_shape_to_send() {
        let schema = json!({
            "type": "object",
            "properties": {
                "actions": {
                    "type": "array",
                    "items": {"type": "object"}
                }
            },
            "required": ["actions"]
        });
        let error = validate_schema_value(
            &schema,
            &schema,
            &json!({"actions": "[{\"tool\":\"team_create\",\"input\":{}}]"}),
            "$",
        )
        .expect_err("a string where the schema declares an array must be refused");
        assert_eq!(
            error,
            "$.actions: value has the wrong JSON type (declared type: array; received: string; \
             send a array value as raw JSON, not as a JSON-encoded string)"
        );
    }

    /// A field the schema types as `string` is never told to become a
    /// container: legitimate JSON text in a string field stays legitimate.
    #[test]
    fn a_string_field_is_never_given_container_guidance() {
        let schema = json!({
            "type": "object",
            "properties": {"content": {"type": "string"}},
            "required": ["content"]
        });
        let error = validate_schema_value(&schema, &schema, &json!({"content": 7}), "$")
            .expect_err("an integer where the schema declares a string must be refused");
        assert_eq!(
            error,
            "$.content: value has the wrong JSON type (declared type: string; received: integer; \
             send a string value)"
        );
    }

    /// A union type is reported as the union, and the guidance still fits.
    #[test]
    fn a_union_type_refusal_names_every_declared_type() {
        let schema = json!({
            "type": "object",
            "properties": {"ref": {"type": ["string", "null"]}},
            "required": ["ref"]
        });
        let error = validate_schema_value(&schema, &schema, &json!({"ref": []}), "$")
            .expect_err("an array where the schema declares string-or-null must be refused");
        assert_eq!(
            error,
            "$.ref: value has the wrong JSON type (declared type: string or null; received: array; \
             send a string or null value)"
        );
    }

    #[test]
    fn write_requires_idempotency_key() {
        let host = test_host("workspace", "/tmp/workspace");
        let lease = host.pin_snapshot();
        let descriptor = lease.describe_effect(
            "edit",
            &json!({"action": "write", "path": "/tmp/tool-host-test", "content": "x"}),
        );
        assert_eq!(
            descriptor.effect_kind,
            harness_contract::tool::ToolEffectKind::Write
        );
        assert_eq!(descriptor.idempotency, ToolIdempotency::IdempotentWithKey);
        let error = lease
            .execute(
                &authorization(&descriptor, None),
                "edit",
                &json!({"action": "write", "path": "/tmp/tool-host-test", "content": "x"}),
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
    fn read_tool_cache_hits_and_invalidates_after_write() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = std::env::temp_dir().join(format!(
            "cowd-read-cache-{}-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test"),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&temp).unwrap();
        std::fs::write(temp.as_path().join("source.txt"), "alpha").unwrap();
        let host = test_host("authorized-cache", temp.as_path());
        let lease = host.pin_snapshot();
        let input = json!({"path":"source.txt"});
        let auth = authorization(&lease.describe_effect("read_file", &input), None);
        lease.execute(&auth, "read_file", &input).unwrap();
        lease.execute(&auth, "read_file", &input).unwrap();
        assert_eq!(host.cache_stats().hits, 1);
        let mut revised = auth.clone();
        revised.policy_revision = 2;
        revised.authorization_lease.policy_revision = 2;
        lease.execute(&revised, "read_file", &input).unwrap();
        assert_eq!(
            host.cache_stats().hits,
            1,
            "new policy revision cannot hit old scope cache"
        );
        let mut revoked = auth.clone();
        revoked.authorization_lease.remaining_uses = 0;
        assert!(lease.execute(&revoked, "read_file", &input).is_err());
        assert_eq!(
            host.cache_stats().hits,
            1,
            "authorization is checked before cache lookup"
        );
        let write = json!({"action":"write","path":"source.txt","content":"omega"});
        let write_auth = authorization(
            &lease.describe_effect("edit", &write),
            Some("write-cache-test"),
        );
        lease.execute(&write_auth, "edit", &write).unwrap();
        assert_eq!(host.cache_stats().invalidations, 1);
        assert!(lease
            .execute(&auth, "read_file", &input)
            .unwrap()
            .contains("omega"));
        lease.execute(&write_auth, "edit", &write).unwrap();
        assert_eq!(
            host.cache_stats().invalidations,
            2,
            "same write input still performs its effect; never read-cache reuse"
        );
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn read_tool_cache_misses_after_external_file_change() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = std::env::temp_dir().join(format!(
            "cowd-read-cache-{}-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test"),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&temp).unwrap();
        let file = temp.as_path().join("source.txt");
        std::fs::write(&file, "alpha").unwrap();
        let old_time = std::fs::metadata(&file).unwrap().modified().unwrap();
        let host = test_host("external-cache", temp.as_path());
        let lease = host.pin_snapshot();
        let input = json!({"path":"source.txt"});
        let auth = authorization(&lease.describe_effect("read_file", &input), None);
        assert!(lease
            .execute(&auth, "read_file", &input)
            .unwrap()
            .contains("alpha"));
        std::fs::write(&file, "omega").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&file)
            .unwrap()
            .set_modified(old_time)
            .unwrap();
        assert!(lease
            .execute(&auth, "read_file", &input)
            .unwrap()
            .contains("omega"));
        assert_eq!(host.cache_stats().hits, 0);
        assert_eq!(host.cache_stats().misses, 2);
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn canonical_mcp_runtime_ids_are_parsed_without_guessing() {
        assert_eq!(
            parse_mcp_runtime_id("mcp__filesystem__read__file"),
            Some(("filesystem", "read__file"))
        );
        assert_eq!(parse_mcp_runtime_id("runtime_tool"), None);
    }

    #[test]
    fn tool_orchestrate_effect_escalates_to_the_strictest_sub_call() {
        use harness_contract::tool::ToolEffectKind;

        let host = ToolHost::builtin("orchestrate-effect", std::env::temp_dir());
        let lease = host.pin_snapshot();

        let read_only = lease.describe_effect(
            "tool_orchestrate",
            &serde_json::json!({
                "steps": [{"id": "g", "op": "call", "tool": "grep_search", "input": {"pattern": "x"}}]
            }),
        );
        assert_eq!(read_only.effect_kind, ToolEffectKind::Read);
        assert_eq!(read_only.required_permission, ToolPermissionMode::ReadOnly);
        assert_eq!(
            read_only.approval_class,
            harness_contract::tool::ToolApprovalClass::None
        );

        let writes = lease.describe_effect(
            "tool_orchestrate",
            &serde_json::json!({
                "steps": [{"id": "w", "op": "call", "tool": "edit", "input": {"action": "write", "path": "x", "content": "y"}}]
            }),
        );
        assert_eq!(writes.effect_kind, ToolEffectKind::Write);
        assert_eq!(
            writes.required_permission,
            ToolPermissionMode::WorkspaceWrite
        );

        // Unparseable input fails closed.
        let invalid = lease.describe_effect("tool_orchestrate", &serde_json::json!({"bad": true}));
        assert_eq!(
            invalid.required_permission,
            ToolPermissionMode::DangerFullAccess
        );
    }

    fn mixed_program(prompt: bool) -> Value {
        let mut program = json!({"steps":[
            {"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"held.txt","content":"real prefix"}},
            {"id":"tail","op":"call","tool":"web_fetch","depends_on":["write"],"input":{"url":"https://example.invalid/r1-no-network-dispatched"}}
        ]});
        if prompt {
            program["steps"][1]["input"]["prompt"] = json!("summarize");
        }
        program
    }

    #[test]
    fn r1_composite_effect_static_metadata_and_authorization_are_complete() {
        use harness_contract::policy::{EffectExternality, PermissionResource};
        use harness_contract::tool::{ToolApprovalClass, ToolEffectKind};
        let host = test_host("composite-scopes", "/tmp/composite-scopes");
        let lease = host.pin_snapshot();
        let original = mixed_program(false);
        let valid = mixed_program(true);
        let effect = lease.describe_effect("tool_orchestrate", &original);
        assert_eq!(
            crate::requested_sub_call_tools("tool_orchestrate", &original),
            crate::SubCallAuthorization::Requires(vec!["edit".into(), "web_fetch".into()])
        );
        assert_eq!(effect.scopes.len(), 4);
        assert!(effect
            .scopes
            .iter()
            .any(|scope| scope.resource == PermissionResource::File
                && scope.target.as_deref() == Some("held.txt")));
        assert!(effect
            .scopes
            .iter()
            .any(|scope| scope.resource == PermissionResource::Network
                && scope.target.as_deref()
                    == Some("https://example.invalid/r1-no-network-dispatched")));
        assert_eq!(effect.effect_kind, ToolEffectKind::Write);
        assert_eq!(
            effect.required_permission,
            ToolPermissionMode::WorkspaceWrite
        );
        assert_eq!(effect.approval_class, ToolApprovalClass::Policy);
        assert_eq!(effect.idempotency, ToolIdempotency::Unknown);
        assert!(effect.uses_network && !effect.spawns_process);
        assert!(!effect.mutates_packages && !effect.mutates_system);
        assert_eq!(
            effect.assessment.externality,
            EffectExternality::NetworkRead
        );
        let valid_effect = lease.describe_effect("tool_orchestrate", &valid);
        assert_eq!(effect.scopes, valid_effect.scopes);
        assert_ne!(effect.descriptor_hash, valid_effect.descriptor_hash);
        lease
            .validate_input("web_fetch", &valid["steps"][1]["input"])
            .unwrap();
        assert!(lease
            .validate_input("web_fetch", &original["steps"][1]["input"])
            .is_err());
        let invocation =
            lease.prepare_governed_invocation("mixed", "tool_orchestrate", &original, &[]);
        assert_eq!(invocation.resource_demand.network_slots, 1);
        assert_eq!(invocation.resource_demand.scopes.len(), 4);
        let auth = authorization(&effect, Some("mixed-key"));
        lease
            .validate_authorization(&auth, "tool_orchestrate", &original)
            .unwrap();
        for index in 0..effect.scopes.len() {
            let mut missing = auth.clone();
            missing.authorization_lease.scopes.remove(index);
            assert_eq!(
                lease
                    .validate_authorization(&missing, "tool_orchestrate", &original)
                    .unwrap_err(),
                ToolHostError::ScopeNotAuthorized
            );
        }
        let mut readonly = auth.clone();
        readonly.authorization_lease.ceiling = ToolPermissionMode::ReadOnly;
        assert!(matches!(
            lease.validate_authorization(&readonly, "tool_orchestrate", &original),
            Err(ToolHostError::LeasePermissionDenied { .. })
        ));
        for status in [
            harness_contract::policy::AuthorizationLeaseStatus::Expired,
            harness_contract::policy::AuthorizationLeaseStatus::Revoked,
        ] {
            let mut inactive = auth.clone();
            inactive.authorization_lease.status = status;
            assert!(matches!(
                lease.validate_authorization(&inactive, "tool_orchestrate", &original),
                Err(ToolHostError::InactiveLease { .. })
            ));
        }
        let mut expired = auth.clone();
        expired.authorization_lease.expires_at_ms = 1;
        assert!(matches!(
            lease.validate_authorization(&expired, "tool_orchestrate", &original),
            Err(ToolHostError::InactiveLease { .. })
        ));
        let mut forged = auth.clone();
        forged.authorization_lease.signature = "forged".into();
        assert_eq!(
            lease
                .validate_authorization(&forged, "tool_orchestrate", &original)
                .unwrap_err(),
            ToolHostError::InvalidLeaseSignature
        );
        let mut wrong_key = auth.clone();
        wrong_key.idempotency_key = Some("other-key".into());
        assert_eq!(
            lease
                .validate_authorization(&wrong_key, "tool_orchestrate", &original)
                .unwrap_err(),
            ToolHostError::LeaseIdempotencyMismatch
        );
        let mut wrong_hash = auth.clone();
        wrong_hash.descriptor_hash = "wrong".into();
        wrong_hash.authorization_lease.effect_descriptor_hash = "wrong".into();
        assert!(matches!(
            lease.validate_authorization(&wrong_hash, "tool_orchestrate", &original),
            Err(ToolHostError::EffectEscalated { .. })
        ));
        for changed in [
            {
                let mut changed = original.clone();
                changed["steps"][0]["input"]["path"] = json!("other.txt");
                changed
            },
            {
                let mut changed = original.clone();
                changed["steps"][0]["input"]["content"] = json!("different");
                changed
            },
            {
                let mut changed = original.clone();
                changed["steps"][1]["input"]["url"] = json!("https://other.invalid");
                changed
            },
        ] {
            assert!(matches!(
                lease.validate_authorization(&auth, "tool_orchestrate", &changed),
                Err(ToolHostError::EffectEscalated { .. })
            ));
        }
    }

    #[test]
    fn original_static_discovery_network_and_write_preserve_every_signed_dimension() {
        use harness_contract::policy::{PermissionOperation, PermissionResource};
        let lease =
            test_host("static-family-frontier", "/tmp/static-family-frontier").pin_snapshot();
        for (tool, input, read_targets) in [
            (
                "glob_search",
                json!({"path":"src","pattern":"*.rs"}),
                vec!["src"],
            ),
            (
                "glob_search",
                json!({"patterns":[{"path":"src","pattern":"*.rs"},{"path":"tests","pattern":"*.rs"}]}),
                vec!["src", "tests"],
            ),
            (
                "grep_search",
                json!({"path":"src","pattern":"needle"}),
                vec!["src"],
            ),
            (
                "grep_search",
                json!({"searches":[{"path":"src","pattern":"needle"},{"path":"tests","pattern":"needle"}]}),
                vec!["src", "tests"],
            ),
            (
                "workspace_snapshot",
                json!({"roots":["src","tests"]}),
                vec!["src", "tests"],
            ),
        ] {
            let program = json!({"steps":[
                {"id":"discover","op":"call","tool":tool,"input":input},
                {"id":"remote","op":"call","tool":"web_fetch","input":{"url":"https://example.invalid","prompt":"summarize"}},
                {"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"out.txt","content":"x"}}
            ]});
            let effect = lease.describe_effect("tool_orchestrate", &program);
            assert_eq!(
                effect.effect_kind,
                harness_contract::tool::ToolEffectKind::Write
            );
            assert!(effect.uses_network && !effect.spawns_process);
            // Static discovery inputs keep their precise signed read frontier;
            // only an escaping pattern or unknown input widens to the workspace.
            for target in &read_targets {
                assert!(
                    effect
                        .scopes
                        .iter()
                        .any(|scope| scope.resource == PermissionResource::File
                            && scope.operation == PermissionOperation::Read
                            && scope.target.as_deref() == Some(*target)),
                    "{tool}: read {target}"
                );
            }
            for (resource, operation, target) in [
                (
                    PermissionResource::File,
                    PermissionOperation::Read,
                    Some("src"),
                ),
                (
                    PermissionResource::File,
                    PermissionOperation::Write,
                    Some("out.txt"),
                ),
                (
                    PermissionResource::File,
                    PermissionOperation::Write,
                    Some(".cowd/checkpoints"),
                ),
                (
                    PermissionResource::Network,
                    PermissionOperation::Call,
                    Some("https://example.invalid"),
                ),
            ] {
                assert!(
                    effect.scopes.iter().any(|scope| scope.resource == resource
                        && scope.operation == operation
                        && scope.target.as_deref() == target),
                    "{tool}: {resource:?}/{operation:?}/{target:?}"
                );
            }
            assert!(effect
                .scopes
                .iter()
                .any(|scope| scope.resource == PermissionResource::Tool));
            let auth = authorization(&effect, None);
            lease
                .validate_authorization(&auth, "tool_orchestrate", &program)
                .unwrap();
            for index in 0..effect.scopes.len() {
                let mut missing = auth.clone();
                missing.authorization_lease.scopes.remove(index);
                assert_eq!(
                    lease
                        .validate_authorization(&missing, "tool_orchestrate", &program)
                        .unwrap_err(),
                    ToolHostError::ScopeNotAuthorized
                );
            }
            let mut changed = program.clone();
            changed["steps"][2]["input"]["path"] = json!("different.txt");
            assert_ne!(
                effect.descriptor_hash,
                lease
                    .describe_effect("tool_orchestrate", &changed)
                    .descriptor_hash
            );
        }
    }

    #[test]
    fn original_static_and_dynamic_discovery_share_the_workspace_file_envelope() {
        use harness_contract::policy::{PermissionOperation, PermissionResource};
        let lease = test_host("coherent-discovery", "/tmp/coherent-discovery").pin_snapshot();
        for (tool, literal, dynamic) in [
            (
                "glob_search",
                json!({"pattern":"*.rs"}),
                json!({"path":"$prior.root","pattern":"*.rs"}),
            ),
            (
                "grep_search",
                json!({"pattern":"needle"}),
                json!({"path":"$prior.root","pattern":"needle"}),
            ),
            (
                "workspace_snapshot",
                json!({}),
                json!({"roots":"$prior.roots"}),
            ),
        ] {
            let static_effect = lease.describe_effect(tool, &literal);
            let program =
                json!({"steps":[{"id":"discover","op":"call","tool":tool,"input":dynamic}]});
            let dynamic_effect = lease.describe_effect("tool_orchestrate", &program);
            for effect in [&static_effect, &dynamic_effect] {
                let files = effect
                    .scopes
                    .iter()
                    .filter(|scope| scope.resource == PermissionResource::File)
                    .collect::<Vec<_>>();
                assert_eq!(files.len(), 1);
                assert_eq!(files[0].operation, PermissionOperation::Read);
                assert_eq!(files[0].target, None);
                assert!(!effect.uses_network && !effect.spawns_process);
                assert!(effect
                    .scopes
                    .iter()
                    .any(|scope| scope.resource == PermissionResource::Tool));
            }
            assert_ne!(
                static_effect.descriptor_hash,
                dynamic_effect.descriptor_hash
            );
            let demand = resource_demand_for_effect(&static_effect);
            assert!(demand
                .scopes
                .iter()
                .any(|scope| scope.key == "." && scope.access == ResourceAccess::Read));
        }
    }

    #[test]
    fn original_lsp_root_not_the_query_path_bounds_the_file_effect() {
        use harness_contract::policy::{PermissionOperation, PermissionResource};
        let root =
            std::env::temp_dir().join(format!("cowd-lsp-footprint-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let lease = test_host("lsp-footprint", &root).pin_snapshot();
        lease.snapshot().lsp.register_command(
            "rust",
            root.to_str(),
            vec![],
            vec!["not-executed".into()],
        );
        let input = json!({"action":"hover","path":"src/a.rs","line":0,"character":0});
        let effect = lease.describe_effect("lsp", &input);
        assert!(effect.spawns_process && !effect.uses_network);
        assert!(effect
            .scopes
            .iter()
            .any(|scope| scope.resource == PermissionResource::File
                && scope.operation == PermissionOperation::Read
                && scope.target.is_none()));
        assert!(!effect
            .scopes
            .iter()
            .any(|scope| scope.resource == PermissionResource::File
                && scope.target.as_deref() == Some("src/a.rs")));
        let program = json!({"steps":[{"id":"lsp","op":"call","tool":"lsp","input":input}]});
        let files = effect
            .scopes
            .iter()
            .find(|scope| scope.resource == PermissionResource::File)
            .unwrap();
        assert!(lease
            .describe_effect("tool_orchestrate", &program)
            .scopes
            .contains(files));
        let diagnostics =
            lease.describe_effect("lsp", &json!({"action":"diagnostics","path":"src/a.rs"}));
        assert!(!diagnostics.spawns_process);
        assert!(diagnostics
            .scopes
            .iter()
            .all(|scope| scope.resource != PermissionResource::File));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn r1_composite_effect_dynamic_inputs_cannot_claim_exact_scope() {
        use harness_contract::policy::{DataClassification, PermissionResource};
        use harness_contract::tool::ToolEffectKind;
        let root =
            std::env::temp_dir().join(format!("cowd-dynamic-envelope-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let host = test_host("dynamic-envelope", &root);
        let lease = host.pin_snapshot();
        for input in [
            json!({"action":"write","path":"fixed.txt","content":"$prior.content"}),
            json!({"action":"write","path":"fixed.txt","content":{"nested":["$item.content"]}}),
        ] {
            let program = json!({"steps":[{"id":"data","op":"map","over":"$prior.items","tool":"edit","input":input}]});
            let effect = lease.describe_effect("tool_orchestrate", &program);
            assert_eq!(effect.effect_kind, ToolEffectKind::Write);
            assert!(effect
                .scopes
                .iter()
                .any(|scope| scope.resource == PermissionResource::File
                    && scope.target.as_deref() == Some("fixed.txt")));
            assert_eq!(
                effect.assessment.data_sensitivity,
                DataClassification::Secret
            );
            lease
                .validate_authorization(&authorization(&effect, None), "tool_orchestrate", &program)
                .unwrap();
        }
        for input in [
            json!({"action":"write","path":"$item.path","content":"x"}),
            json!({"action":"$prior.action","path":"fixed.txt","content":"x"}),
            json!("$prior.input"),
        ] {
            let program = json!({"steps":[{"id":"bounded","op":"map","over":"$prior.items","tool":"edit","input":input}]});
            let effect = lease.describe_effect("tool_orchestrate", &program);
            assert_eq!(effect.effect_kind, ToolEffectKind::Write);
            assert!(effect
                .scopes
                .iter()
                .all(|scope| scope.resource == PermissionResource::File));
            assert_eq!(
                effect
                    .scopes
                    .iter()
                    .filter(|scope| scope.target.is_none())
                    .count(),
                2
            );
            assert_eq!(effect.scopes.len(), 3);
            assert_eq!(
                effect.assessment.data_sensitivity,
                DataClassification::Secret
            );
            let mut exact_only = authorization(&effect, None);
            for scope in &mut exact_only.authorization_lease.scopes {
                scope.target = Some("fixed.txt".into());
            }
            assert_eq!(
                lease
                    .validate_authorization(&exact_only, "tool_orchestrate", &program)
                    .unwrap_err(),
                ToolHostError::ScopeNotAuthorized
            );
        }
        for (tool, input) in [
            ("bash", json!({"command":"$prior.command"})),
            (
                "web_fetch",
                json!({"url":"$prior.url","prompt":"summarize"}),
            ),
            ("mcp", json!({"action":"$item.action","server":"server"})),
        ] {
            let program = json!({"steps":[
                {"id":"prefix","op":"call","tool":"edit","input":{"action":"write","path":"must-not-exist","content":"x"}},
                {"id":"unbounded","op":"call","tool":tool,"input":input}
            ]});
            let effect = lease.describe_effect("tool_orchestrate", &program);
            assert_eq!(effect.effect_kind, ToolEffectKind::Unknown);
            assert!(effect.scopes.iter().all(|scope| scope.target.is_none()));
            assert!(matches!(
                lease.validate_authorization(
                    &authorization(&effect, None),
                    "tool_orchestrate",
                    &program
                ),
                Err(ToolHostError::InputContract { .. })
            ));
            assert!(
                crate::executor::orchestrate::run_orchestrate(&lease, &program)
                    .unwrap_err()
                    .contains("cannot prove")
            );
            assert!(!root.join("must-not-exist").exists());
        }
        for literal in ["$(date)", "$HOME/x", "$$item.path"] {
            let program = json!({"steps":[{"id":"literal","op":"call","tool":"edit","input":{"action":"write","path":literal,"content":"x"}}]});
            let effect = lease.describe_effect("tool_orchestrate", &program);
            assert!(effect
                .scopes
                .iter()
                .any(|scope| scope.resource == PermissionResource::File
                    && scope.target.as_deref() == Some(literal)));
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn composite_repeated_targets_skipped_effects_compute_and_safety_survive() {
        use harness_contract::policy::{
            DataClassification, PermissionOperation, PermissionResource,
        };
        use harness_contract::tool::ToolApprovalClass;
        let lease = test_host("potential-scopes", "/tmp/potential-scopes").pin_snapshot();
        let program = json!({"steps":[
            {"id":"edits","op":"batch","calls":[
                {"tool":"edit","input":{"action":"write","path":"b","content":"x"}},
                {"tool":"edit","input":{"action":"write","path":"a","content":"x"}},
                {"tool":"edit","input":{"action":"write","path":"a","content":"x"}}
            ]},
            {"id":"skip","op":"when","if":{"ref":"$edits","present":false},"tool":"web_fetch","input":{"url":"https://skipped.invalid","prompt":"x"}},
            {"id":"zero","op":"repeat","max":0,"while":{"ref":"$edits","present":true},"tool":"edit","input":{"action":"write","path":".env","content":"x"}},
            {"id":"reduce","op":"compute","language":"python","code":"print(_input)","input":"$edits"}
        ]});
        let effect = lease.describe_effect("tool_orchestrate", &program);
        assert_eq!(
            effect
                .scopes
                .iter()
                .filter(|scope| scope.resource == PermissionResource::File
                    && scope.operation == PermissionOperation::Write)
                .count(),
            4
        );
        assert!(effect
            .scopes
            .iter()
            .any(|scope| scope.target.as_deref() == Some("https://skipped.invalid")));
        assert!(!effect
            .scopes
            .iter()
            .any(|scope| scope.resource == PermissionResource::Shell
                && scope.operation == PermissionOperation::Execute));
        assert!(!effect
            .scopes
            .iter()
            .any(|scope| scope.resource == PermissionResource::File
                && scope.operation == PermissionOperation::Read
                && scope.target.is_none()));
        assert!(effect.uses_network && effect.spawns_process);
        assert_eq!(
            effect.assessment.data_sensitivity,
            DataClassification::Secret
        );
        let preview = json!({"steps":[{"id":"p","op":"call","tool":"edit","input":{"action":"preview","edits":[{"path":"a"}]}}]});
        assert_eq!(
            lease
                .describe_effect("tool_orchestrate", &preview)
                .required_permission,
            ToolPermissionMode::WorkspaceWrite
        );
        let dangerous = json!({"steps":[{"id":"p","op":"call","tool":"bash","input":{"command":"sudo npm install package"}}]});
        let effect = lease.describe_effect("tool_orchestrate", &dangerous);
        assert_eq!(effect.approval_class, ToolApprovalClass::Administrator);
        assert!(effect.spawns_process && effect.mutates_packages && effect.mutates_system);
        let network = json!({"steps":[{"id":"p","op":"call","tool":"web_fetch","input":{"url":"https://read.invalid","prompt":"x"}}]});
        let effect = lease.describe_effect("tool_orchestrate", &network);
        assert_eq!(effect.required_permission, ToolPermissionMode::ReadOnly);
        assert_eq!(effect.approval_class, ToolApprovalClass::Policy);
        assert!(effect.uses_network);
    }

    #[test]
    fn composite_real_file_prefix_and_failed_tail_keep_original_error_trace() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root =
            std::env::temp_dir().join(format!("cowd-composite-partial-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let lease = test_host("real-partial", &root).pin_snapshot();
        let program = mixed_program(false);
        let effect = lease.describe_effect("tool_orchestrate", &program);
        let error = lease
            .execute(
                &authorization(&effect, Some("partial-key")),
                "tool_orchestrate",
                &program,
            )
            .unwrap_err()
            .to_string();
        assert_eq!(
            std::fs::read_to_string(root.join("held.txt")).unwrap(),
            "real prefix"
        );
        assert!(error.contains("prompt") && error.contains("tail") && error.contains("trace:"));
        assert!(error.contains("\"status\":\"error\"") && error.contains("\"status\":\"ok\""));
        let collect_program = json!({"steps":[{"id":"batch","op":"batch","on_error":"collect","calls":[
            {"tool":"read_file","as":"found","input":{"path":"held.txt"}},
            {"tool":"read_file","as":"missing","input":{"path":"missing.txt"}}
        ]}]});
        let effect = lease.describe_effect("tool_orchestrate", &collect_program);
        let output = lease
            .execute(
                &authorization(&effect, None),
                "tool_orchestrate",
                &collect_program,
            )
            .unwrap();
        let output: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(output["steps"]["batch"]["found"]["ok"], true);
        assert_eq!(output["steps"]["batch"]["missing"]["ok"], false);
        assert!(output["steps"]["batch"]["missing"]["error"].is_string());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn composite_original_dynamic_map_flow_and_data_write_keep_their_bounded_envelope() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        use harness_contract::policy::PermissionResource;
        let root =
            std::env::temp_dir().join(format!("cowd-composite-map-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a.txt"), "alpha").unwrap();
        std::fs::write(root.join("b.txt"), "gamma").unwrap();
        let lease = test_host("original-dynamic-map", &root).pin_snapshot();
        let program = json!({"steps":[
            {"id":"g","op":"call","tool":"glob_search","input":{"pattern":"*.txt"}},
            {"id":"files","op":"map","over":"$g.filenames","tool":"read_file","input":{"path":"$item"},"field":"file"},
            {"id":"gamma","op":"filter","over":"$files","field":"content","contains":"gamma"},
            {"id":"count","op":"collect","over":"$gamma","agg":"count"},
            {"id":"branch","op":"when","if":{"ref":"$count","equals":1},"tool":"read_file","input":{"path":"b.txt"}},
            {"id":"looped","op":"repeat","max":3,"while":{"ref":"$count","equals":1},"tool":"read_file","input":{"path":"a.txt"}},
            {"id":"data_write","op":"call","tool":"edit","input":{"action":"write","path":"summary.txt","content":"$branch.file.content"}},
            {"id":"skip","op":"when","if":{"ref":"$count","equals":0},"tool":"edit","input":{"action":"write","path":"skipped.txt","content":"never"}},
            {"id":"zero","op":"repeat","max":0,"while":{"ref":"$count","equals":1},"tool":"edit","input":{"action":"write","path":"zero.txt","content":"never"}}
        ],"return":"$count"});
        let effect = lease.describe_effect("tool_orchestrate", &program);
        assert!(effect
            .scopes
            .iter()
            .any(|scope| scope.resource == PermissionResource::File && scope.target.is_none()));
        let output = lease
            .execute(
                &authorization(&effect, Some("dynamic-key")),
                "tool_orchestrate",
                &program,
            )
            .unwrap();
        let output: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(output["result"], 1);
        assert_eq!(output["steps"]["gamma"].as_array().unwrap().len(), 1);
        assert_eq!(
            std::fs::read_to_string(root.join("summary.txt")).unwrap(),
            "gamma"
        );
        assert!(!root.join("skipped.txt").exists() && !root.join("zero.txt").exists());
        let map_collect = json!({"steps":[
            {"id":"g","op":"call","tool":"glob_search","input":{"pattern":"*.txt"}},
            {"id":"mapped","op":"map","over":"$g.filenames","tool":"read_file","input":{"path":"missing.txt"},"on_error":"collect"}
        ]});
        let effect = lease.describe_effect("tool_orchestrate", &map_collect);
        let output = lease
            .execute(
                &authorization(&effect, None),
                "tool_orchestrate",
                &map_collect,
            )
            .unwrap();
        let output: Value = serde_json::from_str(&output).unwrap();
        let receipts = output["steps"]["mapped"].as_array().unwrap();
        assert_eq!(receipts.len(), 3);
        assert!(receipts
            .iter()
            .all(|receipt| receipt["ok"] == false && receipt["error"].is_string()));
        std::fs::write(root.join("action.txt"), "write").unwrap();
        std::fs::write(root.join("target.txt"), "produced.txt").unwrap();
        let dynamic_control = json!({"steps":[
            {"id":"action","op":"call","tool":"read_file","input":{"path":"action.txt"}},
            {"id":"target","op":"call","tool":"read_file","input":{"path":"target.txt"}},
            {"id":"write","op":"call","tool":"edit","input":{"action":"$action.file.content","path":"$target.file.content","content":"bounded"}}
        ]});
        let effect = lease.describe_effect("tool_orchestrate", &dynamic_control);
        lease
            .execute(
                &authorization(&effect, Some("control-key")),
                "tool_orchestrate",
                &dynamic_control,
            )
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("produced.txt")).unwrap(),
            "bounded"
        );
        let escaped = format!("cowd-escape-{}", uuid::Uuid::new_v4());
        std::fs::write(root.join("target.txt"), format!("../{escaped}")).unwrap();
        assert!(lease
            .execute(
                &authorization(&effect, Some("control-key")),
                "tool_orchestrate",
                &dynamic_control
            )
            .is_err());
        assert!(!root.parent().unwrap().join(escaped).exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn composite_pins_catalog_and_rejects_non_dispatchable_children() {
        use harness_contract::tool::{ToolEffectKind, ToolEffectResolverSpec};
        let host = test_host("pinned-children", "/tmp/pinned-children");
        let before = host.pin_snapshot();
        let program = mixed_program(true);
        let effect = before.describe_effect("tool_orchestrate", &program);
        let catalog = ToolCatalog::builtin()
            .with_runtime_tools(vec![crate::RuntimeToolDefinition {
                name: "runtime_only".into(),
                description: Some("not an orchestration builtin".into()),
                input_schema: json!({"type":"object"}),
                required_permission: crate::permissions::PermissionMode::ReadOnly,
                effect_resolver: ToolEffectResolverSpec {
                    resolver_id: "runtime.readonly".into(),
                    resolver_version: 9,
                },
            }])
            .unwrap();
        host.replace_snapshot(ToolHostSnapshot::new(
            Arc::new(catalog),
            Arc::new(LspRegistry::new()),
            None,
        ));
        let after = host.pin_snapshot();
        assert_eq!(before.describe_effect("tool_orchestrate", &program), effect);
        assert_ne!(
            after
                .describe_effect("tool_orchestrate", &program)
                .descriptor_hash,
            effect.descriptor_hash
        );
        let auth = authorization(&effect, Some("pin-key"));
        before
            .validate_authorization(&auth, "tool_orchestrate", &program)
            .unwrap();
        assert!(matches!(
            after.validate_authorization(&auth, "tool_orchestrate", &program),
            Err(ToolHostError::EffectEscalated { .. })
        ));
        for tool in [
            "runtime_only",
            "unknown",
            "tool_orchestrate",
            "web-fetch",
            "$prior.tool",
        ] {
            let program = json!({"steps":[{"id":"bad","op":"call","tool":tool,"input":{}}]});
            let effect = after.describe_effect("tool_orchestrate", &program);
            assert_eq!(effect.effect_kind, ToolEffectKind::Unknown);
            assert!(matches!(
                after.validate_authorization(
                    &authorization(&effect, None),
                    "tool_orchestrate",
                    &program
                ),
                Err(ToolHostError::InputContract { .. })
            ));
        }
    }
}
