use serde::{Deserialize, Serialize};

use harness_contract::execution_graph::ExecutionParentBinding;

use crate::tool_dispatch::ToolRequest;
use crate::tool_orchestrator::ToolSafetyCategory;

/// In-process progress sink for long-running tools such as bash (T4). The
/// wrapper keeps the request struct's derives (Debug/PartialEq) valid while
/// serde always skips the callback; only its presence is ever compared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolLifecycleAnchor {
    pub invocation_id: String,
    pub session_id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub input_hash: String,
    pub producer: harness_contract::projection::RuntimeActivityBinding,
}

#[derive(Clone, Default)]
pub struct ToolProgressSink(
    pub Option<std::sync::Arc<dyn Fn(&str) + Send + Sync>>,
    std::sync::Arc<std::sync::Mutex<Option<Vec<tools::OrchestratedRead>>>>,
    std::sync::Arc<std::sync::Mutex<Option<ToolLifecycleAnchor>>>,
);

impl ToolProgressSink {
    pub fn new(callback: Option<std::sync::Arc<dyn Fn(&str) + Send + Sync>>) -> Self {
        Self(callback, Default::default(), Default::default())
    }
    pub fn record_child_reads(&self, reads: Vec<tools::OrchestratedRead>) -> Result<(), String> {
        let mut slot = self.1.lock().map_err(|_| "child read channel poisoned")?;
        if slot.is_some() { return Err("child read channel already populated".into()); }
        *slot = Some(reads);
        Ok(())
    }
    pub(crate) fn child_reads(&self) -> Result<Option<Vec<tools::OrchestratedRead>>, String> {
        Ok(self.1.lock().map_err(|_| "child read channel poisoned")?.clone())
    }
    pub(crate) fn record_lifecycle_anchor(&self, anchor: ToolLifecycleAnchor) -> Result<(), String> {
        let mut slot = self.2.lock().map_err(|_| "tool lifecycle channel poisoned")?;
        if slot.is_some() { return Err("tool lifecycle anchor already populated".into()); }
        *slot = Some(anchor);
        Ok(())
    }
    pub(crate) fn lifecycle_anchor(&self) -> Result<Option<ToolLifecycleAnchor>, String> {
        Ok(self.2.lock().map_err(|_| "tool lifecycle channel poisoned")?.clone())
    }
}

impl std::fmt::Debug for ToolProgressSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ToolProgressSink")
            .field(&self.0.is_some())
            .finish()
    }
}

impl PartialEq for ToolProgressSink {
    fn eq(&self, other: &Self) -> bool {
        self.0.is_some() == other.0.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeToolExecutionRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_invocation: Option<harness_contract::tool::GovernedToolInvocation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_binding: Option<crate::step_snapshot::StepCallBinding>,
    pub governed_plan_id: String,
    pub governed_plan_revision: u64,
    /// Stable causal wave allocated before execution. Parallel calls in one
    /// ToolBatch share it; retry/replay retains it. It is never reconstructed
    /// from plan revision or post-hoc Goal observations.
    #[serde(default)]
    pub observation_wave_sequence: u64,
    pub idempotency_key: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub input: String,
    pub category: ToolSafetyCategory,
    /// Runtime-issued authorization for an ordinary tool effect. Gateway
    /// control tools do not use this field; delegated Agent tools carry it so
    /// the concrete ToolHost can verify the same descriptor before execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<harness_contract::tool::ToolExecutionAuthorization>,
    /// Logical parent session that owns this graph node. It is not inferred
    /// from a process-global tool host, because a single Gateway serves many
    /// concurrent sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Runtime-derived sandbox boundary for this invocation. It is derived
    /// from the owning Session autonomy profile, never from model input.
    #[serde(default)]
    pub sandbox_posture: harness_contract::policy::SandboxPosture,
    /// Exact Session policy generation paired with `sandbox_posture` and the
    /// authorization lease. Production ToolHosts reject zero or stale values.
    #[serde(default)]
    pub policy_revision: u64,
    /// Runtime-derived evidence/artifact scopes the caller is authorized to
    /// read. Derived from the owning Agent binding and session, never from
    /// model input. Empty means no scoped evidence access.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authorized_scopes: Vec<String>,
    /// Exact Memory read binding compiled by the owning ConversationRuntime.
    ///
    /// Context retrieval must not reconstruct Agent/Definition/Team authority
    /// from a Session id at the Gateway boundary. Primary and delegated
    /// runtimes carry the same lease that passive context assembly uses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_context: Option<memory::MemoryTurnContext>,
    /// Validated Runtime Binding data lease shared with passive Fact/Matrix recall.
    /// Never populated from the model tool input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reality_context: Option<harness_contract::agent::AgentDataLease>,
    /// Selected parent model lease. Runtime control tools inherit this binding
    /// for any child AgentTask graph instead of resolving an imaginary default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_lease: Option<String>,
    /// Canonical parent graph/node that issued this tool call. It is supplied
    /// by the Runner, never parsed from model-generated tool JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution: Option<ExecutionParentBinding>,
    /// Attempt fence for a delegated Agent tool receipt.  Together with the
    /// parent graph/node it gives the durable receipt index an exact owner;
    /// it is never inferred from a tool-use label during recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution_attempt: Option<u32>,
    /// Immutable execution decision for the owning conversation turn. Runtime
    /// supplies this lease so orchestration validation and Team identity never
    /// consult process-global Gateway state in concurrent sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_decision: Option<crate::RuntimeExecutionDecision>,
    /// Runtime-issued paired evaluation traffic. Tool adapters use this to
    /// enforce the evaluation isolation policy rather than trusting a model
    /// supplied session or tool parameter.
    #[serde(default)]
    pub evaluation_isolated: bool,
    /// Runtime-issued Managed Agent invocation fence.  Gateway may execute a
    /// side effect only after Runtime has recorded and claimed the matching
    /// outbox entry; conversational calls always leave this empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_invocation: Option<harness_contract::managed_agent::ManagedAgentInvocationFence>,
    /// Optional per-invocation progress sink used by bash's tokio path (T4).
    /// The callback is in-process only; it is never serialized.
    #[serde(skip)]
    pub tool_progress: ToolProgressSink,
}

impl RuntimeToolExecutionRequest {
    #[must_use]
    pub fn from_tool_request(request: &ToolRequest) -> Self {
        Self {
            compiled_invocation: None,
            step_binding: None,
            governed_plan_id: "offline-unknown".to_string(),
            governed_plan_revision: 0,
            observation_wave_sequence: 0,
            idempotency_key: request.tool_use_id.clone(),
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            input: request.input.clone(),
            category: ToolSafetyCategory::Destructive,
            authorization: None,
            session_id: None,
            // Offline adapters have no Session policy. ReadOnly is the only
            // safe decode/default posture and production admission still
            // requires a positive policy revision plus exact live equality.
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 0,
            authorized_scopes: Vec::new(),
            memory_context: None,
            reality_context: None,
            model_lease: None,
            parent_execution: None,
            parent_execution_attempt: None,
            execution_decision: None,
            evaluation_isolated: false,
            managed_invocation: None,
            tool_progress: ToolProgressSink::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeToolExecutionStatus {
    Executed,
    BlockedPermission,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeToolExecutionOutcome {
    pub tool_use_id: String,
    pub tool_name: String,
    pub status: RuntimeToolExecutionStatus,
    pub category: ToolSafetyCategory,
    pub output: Option<String>,
    pub error: Option<String>,
    pub evidence_ref: String,
    /// Typed facts from the original execution, including bounded process
    /// failures, cancellation and unknown physical outcomes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observed_evidence: Vec<harness_contract::context::ObservedEvidence>,
}

impl RuntimeToolExecutionOutcome {
    pub(crate) fn model_feedback_is_error(&self) -> bool {
        self.status != RuntimeToolExecutionStatus::Executed
            || bounded_process_feedback_is_error(&self.tool_name, &self.observed_evidence)
    }
}

pub(crate) fn bounded_process_feedback_is_error(
    tool_name: &str,
    observations: &[harness_contract::context::ObservedEvidence],
) -> bool {
    use harness_contract::context::{EvidenceTargetIdentity, ToolExecutionStopReason, ToolExecutionTerminal};
    if tool_name != "execute_code" {
        return false;
    }
    let mut facts = observations.iter().filter_map(|observed| match &observed.target {
        EvidenceTargetIdentity::ToolExecution { execution } => Some((observed.tool_name.as_str(), execution)),
        _ => None,
    });
    let Some((observed_tool_name, fact)) = facts.next() else {
        return true;
    };
    facts.next().is_some()
        || observed_tool_name != tool_name
        || !matches!(&fact.terminal,
            ToolExecutionTerminal::Settled {
                exit_code: Some(0),
                signal: None,
                stop_reason: ToolExecutionStopReason::CommandExit,
                ..
            })
}

pub struct BoundedProcessAuthority {
    services: std::sync::Arc<crate::RuntimeServices>,
    request: RuntimeToolExecutionRequest,
    managed_started: bool,
    finalized: bool,
    #[cfg(test)]
    checkpoint: Option<(
        &'static str,
        tokio::sync::oneshot::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
}

tokio::task_local! {
    static BOUNDED_PROCESS_COMMIT_AUTHORITY: (std::sync::Arc<crate::RuntimeServices>,RuntimeToolExecutionRequest);
}

pub(crate) fn check_bounded_process_commit_authority(
    request: &RuntimeToolExecutionRequest,
) -> Result<(), crate::ToolError> {
    BOUNDED_PROCESS_COMMIT_AUTHORITY
        .try_with(|(services, original)| {
            BoundedProcessAuthority::validate_context(services, original, request)
        })
        .map_err(|_| {
            crate::ToolError::new("bounded process terminal requires its owned live authority")
        })?
}

pub(crate) struct BoundedProcessAuthorityMonitor {
    stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Drop for BoundedProcessAuthorityMonitor {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl BoundedProcessAuthority {
    pub fn bind(
        services: std::sync::Arc<crate::RuntimeServices>,
        request: RuntimeToolExecutionRequest,
    ) -> Result<Self, crate::ToolError> {
        let authority = Self {
            services,
            request,
            managed_started: false,
            finalized: false,
            #[cfg(test)]
            checkpoint: None,
        };
        authority.check_context(&authority.request)?;
        Ok(authority)
    }

    #[cfg(test)]
    pub(crate) fn observe_checkpoint_for_test(
        &mut self,
        phase: &'static str,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        assert!(self.checkpoint.is_none());
        let (notify, observed) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        self.checkpoint = Some((phase, notify, released));
        (observed, release)
    }

    #[cfg(test)]
    pub(crate) fn checkpoint_for_test(&mut self, phase: &'static str) {
        if self
            .checkpoint
            .as_ref()
            .is_some_and(|(selected, _, _)| *selected == phase)
        {
            let (_, notify, released) = self.checkpoint.take().unwrap();
            let _ = notify.send(());
            let _ = released.recv();
        }
    }

    pub(crate) fn check_context(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> Result<(), crate::ToolError> {
        Self::validate_context(&self.services, &self.request, request)
    }

    pub(crate) fn shares_store(
        &self,
        service: &crate::execution_core::graph::ExecutionCommitService,
    ) -> bool {
        service.bounded_process_uses_store(self.services.event_store())
    }

    pub(crate) fn shares_plane(&self, plane: &std::sync::Arc<crate::ToolExecutionPlane>) -> bool {
        std::sync::Arc::ptr_eq(self.services.tool_execution_plane(), plane)
    }

    pub(crate) async fn with_commit_context<Output, Operation>(
        &self,
        operation: Operation,
    ) -> Output
    where
        Operation: std::future::Future<Output = Output>,
    {
        BOUNDED_PROCESS_COMMIT_AUTHORITY
            .scope((self.services.clone(), self.request.clone()), operation)
            .await
    }

    fn validate_context(
        services: &crate::RuntimeServices,
        original: &RuntimeToolExecutionRequest,
        request: &RuntimeToolExecutionRequest,
    ) -> Result<(), crate::ToolError> {
        let error = |message: &str| crate::ToolError::new(message);
        if serde_json::to_value(original).map_err(|failure| error(&failure.to_string()))?
            != serde_json::to_value(request).map_err(|failure| error(&failure.to_string()))?
        {
            return Err(error("bounded process immutable authority request changed"));
        }
        let session = request
            .session_id
            .as_deref()
            .ok_or_else(|| error("bounded process Session missing"))?;
        let policy = services
            .session_execution_policy(session)
            .ok_or_else(|| error("bounded process live Session policy missing"))?;
        if request.policy_revision == 0
            || request.policy_revision != policy.revision
            || request.sandbox_posture != policy.sandbox_posture
        {
            return Err(error(
                "bounded process live Session authority revoked or changed",
            ));
        }
        if let Some(fence) = &request.managed_invocation {
            fence
                .validate()
                .map_err(|failure| error(&failure.to_string()))?;
            let invocation = services
                .managed_agents()
                .invocations()
                .map_err(|failure| error(&failure))?
                .into_iter()
                .find(|invocation| invocation.invocation_id == fence.invocation_id)
                .ok_or_else(|| error("bounded managed invocation missing"))?;
            if invocation.status != crate::ManagedAgentInvocationStatus::Running
                || invocation.fence_generation != fence.fence_generation
                || invocation.attempt_no != fence.attempt_no
                || invocation.definition_id != fence.managed_agent_id
                || invocation.definition_revision != fence.definition_revision
                || invocation.claimed_by.as_deref() != Some(fence.dispatcher_id.as_str())
            {
                return Err(error("bounded process managed invocation fence revoked"));
            }
        }
        Ok(())
    }

    pub(crate) fn check(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> Result<(), crate::ToolError> {
        self.check_context(request)?;
        let effect = &request
            .compiled_invocation
            .as_ref()
            .ok_or_else(|| crate::ToolError::new("bounded process compiled effect missing"))?
            .effect;
        self.services
            .commit_service()
            .bounded_process_binding(request, effect)
            .map_err(|error| crate::ToolError::new(error.to_string()))?;
        Ok(())
    }

    pub(crate) fn check_settlement(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> Result<(), crate::ToolError> {
        // Session/managed immutable context checks remain. Only fresh Root /
        // Program running authority is replaced by the original admission.
        self.check_context(request)?;
        let effect = &request
            .compiled_invocation
            .as_ref()
            .ok_or_else(|| crate::ToolError::new("Process settlement compiled effect missing"))?
            .effect;
        self.services
            .commit_service()
            .bounded_process_settlement_binding(request, effect)
            .map(|_| ())
            .map_err(|error| crate::ToolError::new(error.to_string()))
    }

    pub(crate) fn begin(
        &mut self,
        request: &RuntimeToolExecutionRequest,
    ) -> Result<(), crate::ToolError> {
        self.check(request)?;
        if self.managed_started {
            return Err(crate::ToolError::new(
                "bounded managed permit already consumed",
            ));
        }
        if let Some(fence) = &request.managed_invocation {
            let effect_id = format!("tool:{}:{}", request.tool_name, request.tool_use_id);
            match self
                .services
                .begin_managed_agent_effect(
                    fence,
                    &effect_id,
                    format!("runtime_tool:{:?}", request.category).to_ascii_lowercase(),
                    request.idempotency_key.clone(),
                    format!(
                        "runtime-tool:{}:{}",
                        request.tool_name, request.idempotency_key
                    ),
                )
                .map_err(|error| crate::ToolError::new(error.to_string()))?
            {
                crate::ManagedAgentEffectPermit::Execute { .. } => self.managed_started = true,
                crate::ManagedAgentEffectPermit::AlreadyCompleted { .. } => {
                    return Err(crate::ToolError::new(
                        "bounded managed effect already completed; refusing new execution",
                    ))
                }
            }
        }
        Ok(())
    }

    pub(crate) fn monitor(
        &self,
        request: &RuntimeToolExecutionRequest,
        cancellation: tools::ToolExecutionCancellation,
    ) -> Result<BoundedProcessAuthorityMonitor, crate::ToolError> {
        self.check(request)?;
        let services = self.services.clone();
        let original = self.request.clone();
        let request = request.clone();
        let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let captured = stopped.clone();
        let worker = std::thread::Builder::new()
            .name("bounded-process-authority".into())
            .spawn(move || {
                while !captured.load(std::sync::atomic::Ordering::Acquire) {
                    let valid =
                        Self::validate_context(&services, &original, &request).and_then(|()| {
                            let effect = &request
                                .compiled_invocation
                                .as_ref()
                                .ok_or_else(|| {
                                    crate::ToolError::new("bounded process compiled effect missing")
                                })?
                                .effect;
                            services
                                .commit_service()
                                .bounded_process_binding(&request, effect)
                                .map(|_| ())
                                .map_err(|error| crate::ToolError::new(error.to_string()))
                        });
                    if valid.is_err() {
                        cancellation.cancel();
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            })
            .map_err(|error| crate::ToolError::new(error.to_string()))?;
        Ok(BoundedProcessAuthorityMonitor {
            stopped,
            worker: Some(worker),
        })
    }

    pub(crate) fn complete(
        &mut self,
        request: &RuntimeToolExecutionRequest,
        receipt: &str,
    ) -> Result<(), crate::ToolError> {
        if self.finalized {
            return Err(crate::ToolError::new(
                "bounded process lifecycle already finalized",
            ));
        }
        self.check_settlement(request)?;
        if self.managed_started {
            self.services
                .complete_managed_agent_effect(
                    request.managed_invocation.as_ref().unwrap(),
                    &format!("tool:{}:{}", request.tool_name, request.tool_use_id),
                    receipt.into(),
                )
                .map_err(|error| crate::ToolError::new(error.to_string()))?;
        }
        self.finalized = true;
        Ok(())
    }

    pub(crate) fn reconcile(&mut self, reason: String) {
        if self.finalized {
            return;
        }
        self.finalized = true;
        if self.managed_started {
            let _ = self.services.reconcile_managed_agent_effect(
                self.request.managed_invocation.as_ref().unwrap(),
                &format!(
                    "tool:{}:{}",
                    self.request.tool_name, self.request.tool_use_id
                ),
                reason,
            );
        }
    }
}

impl Drop for BoundedProcessAuthority {
    fn drop(&mut self) {
        self.reconcile(
            "bounded physical operation ended without an authoritative terminal completion".into(),
        );
    }
}

/// Thin adapter used by the canonical ToolBatch node executor.
///
/// Graph scheduling, dependency resolution, policy, retries, and lifecycle
/// commits are deliberately outside this contract and belong to the Runner.
#[async_trait::async_trait]
pub trait RuntimeExecutionHost: Send + Sync {
    fn bounded_process_authority(
        &self,
        _request: &RuntimeToolExecutionRequest,
    ) -> Result<BoundedProcessAuthority, crate::ToolError> {
        Err(crate::ToolError::new(
            "bounded process live authority is unsupported by this host",
        ))
    }
    fn bounded_process_lease(
        &self,
        _request: &RuntimeToolExecutionRequest,
    ) -> Result<tools::ToolHostLease, crate::ToolError> {
        Err(crate::ToolError::new(
            "bounded process execution is unsupported by this host",
        ))
    }

    fn bounded_process_plane(&self) -> Option<std::sync::Arc<crate::ToolExecutionPlane>> {
        None
    }

    fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
        None
    }

    async fn execute_runtime_tool(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> RuntimeToolExecutionOutcome;

    fn delegated_tool_definitions(
        &self,
        tool_names: &[String],
    ) -> Vec<crate::ProviderToolDefinition> {
        let Some(lease) = self.pin_step_catalog() else {
            return Vec::new();
        };
        let requested = tool_names.iter().cloned().collect();
        lease
            .snapshot()
            .catalog
            .definitions(Some(&requested))
            .into_iter()
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect()
    }

    /// Return the concrete host descriptor used to authorize a delegated
    /// tool. `None` means that the current host snapshot cannot execute it.
    fn delegated_tool_effect_descriptor(
        &self,
        _tool_name: &str,
        _input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        None
    }
}

/// Whether a concrete effect descriptor is safe to carry through a bounded
/// delegated Team lease. Admission and Agent activation must share this
/// predicate or the latter can silently erase tools promised by the former.
#[must_use]
pub fn delegated_tool_effect_is_bounded(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
) -> bool {
    use harness_contract::policy::{EffectBlastRadius, EffectExternality, EffectReversibility};
    use harness_contract::tool::{ToolApprovalClass, ToolEffectKind, ToolPermissionMode};

    if descriptor.mutates_packages
        || descriptor.mutates_system
        || matches!(
            descriptor.effect_kind,
            ToolEffectKind::Package
                | ToolEffectKind::System
                | ToolEffectKind::Destructive
                | ToolEffectKind::Unknown
        )
    {
        return false;
    }
    if !descriptor.spawns_process {
        return true;
    }

    // Process creation is delegable only when the concrete registered
    // effect proves a workspace-bounded sandbox. Arbitrary shell commands
    // remain DangerFullAccess/User effects and therefore fail this gate;
    // a model cannot manufacture these descriptor fields.
    !descriptor.uses_network
        && descriptor.required_permission != ToolPermissionMode::DangerFullAccess
        && matches!(
            descriptor.approval_class,
            ToolApprovalClass::None | ToolApprovalClass::Policy
        )
        && matches!(
            descriptor.assessment.externality,
            EffectExternality::Internal | EffectExternality::Workspace
        )
        && matches!(
            descriptor.assessment.blast_radius,
            EffectBlastRadius::Item | EffectBlastRadius::Workspace
        )
        && matches!(
            descriptor.assessment.reversibility,
            EffectReversibility::Reversible | EffectReversibility::Compensatable
        )
}

pub(crate) fn effect_file_accesses(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
) -> Vec<(String, bool)> {
    use harness_contract::policy::{PermissionOperation, PermissionResource};

    let mut accesses = descriptor
        .scopes
        .iter()
        .filter(|scope| scope.resource == PermissionResource::File)
        .map(|scope| {
            (
                scope
                    .target
                    .as_deref()
                    .filter(|target| !target.trim().is_empty())
                    .unwrap_or(".")
                    .to_string(),
                scope.operation != PermissionOperation::Read,
            )
        })
        .collect::<Vec<_>>();
    if descriptor.spawns_process {
        accesses.push((".".to_string(), false));
    }
    accesses.sort();
    accesses.dedup();
    accesses
}

pub(crate) fn effect_has_resource_lease(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
    allowed_scopes: &[String],
    path_authorized: impl FnMut(&str, &[String], bool) -> bool,
) -> bool {
    delegated_tool_effect_is_bounded(descriptor)
        && effect_resource_grants_cover(descriptor, allowed_scopes, path_authorized)
}

fn effect_resource_grants_cover(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
    allowed_scopes: &[String],
    mut path_authorized: impl FnMut(&str, &[String], bool) -> bool,
) -> bool {
    use harness_contract::policy::{PermissionOperation, PermissionResource};

    (!descriptor.uses_network || allowed_scopes.iter().any(|scope| scope == "network:*"))
        && descriptor.scopes.iter().all(|scope| {
            scope.resource != PermissionResource::File
                || matches!(
                    scope.operation,
                    PermissionOperation::Read | PermissionOperation::Write
                )
        })
        && effect_file_accesses(descriptor)
            .iter()
            .all(|(path, write)| {
                if crate::governed_tool_plan::normalize_resource_path(path) == "." {
                    whole_workspace_resource_lease(allowed_scopes, *write)
                } else {
                    path_authorized(path, allowed_scopes, *write)
                }
            })
}

pub(crate) fn whole_workspace_resource_lease(scopes: &[String], write: bool) -> bool {
    scopes.iter().any(|scope| {
        scope.split_once(':').is_some_and(|(mode, target)| {
            matches!(target.trim(), "." | "./")
                && (matches!(mode, "write" | "workspace") || (!write && mode == "read"))
        })
    })
}

pub(crate) fn effect_resource_evidence(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
) -> Vec<String> {
    let mut scopes = effect_file_accesses(descriptor)
        .into_iter()
        .map(|(path, write)| format!("{}:{path}", if write { "write" } else { "read" }))
        .collect::<Vec<_>>();
    if descriptor.uses_network {
        scopes.push("network:*".to_string());
    }
    scopes.sort();
    scopes.dedup();
    scopes
}

pub(crate) fn lexical_resource_path_is_authorized(
    path: &str,
    scopes: &[String],
    write: bool,
) -> bool {
    let path = crate::governed_tool_plan::normalize_resource_path(path);
    if crate::governed_tool_plan::validate_relative_scope_path(&path).is_err() {
        return false;
    }
    scopes.iter().any(|scope| {
        let Some((mode, target)) = scope.split_once(':') else {
            return false;
        };
        if target.trim().is_empty() {
            return false;
        }
        if !matches!(mode, "write" | "workspace") && (write || mode != "read") {
            return false;
        }
        let target = crate::governed_tool_plan::normalize_resource_path(target);
        crate::governed_tool_plan::validate_relative_scope_path(&target).is_ok()
            && (target == "."
                || path == target
                || path
                    .strip_prefix(&target)
                    .is_some_and(|suffix| suffix.starts_with('/')))
    })
}

pub(crate) struct DelegatedCatalogProjection<'lease> {
    lease: &'lease tools::ToolHostLease,
    metadata: &'lease harness_contract::tool::ToolDiscoveryReceipt,
    registered: std::collections::BTreeMap<
        String,
        (
            crate::ProviderToolDefinition,
            harness_contract::tool::ToolPermissionMode,
        ),
    >,
    descriptors:
        std::collections::BTreeMap<&'lease str, &'lease harness_contract::tool::ToolDescriptorRef>,
}

impl<'lease> DelegatedCatalogProjection<'lease> {
    pub(crate) fn new(
        lease: &'lease tools::ToolHostLease,
        metadata: &'lease harness_contract::tool::ToolDiscoveryReceipt,
        requested: &std::collections::BTreeSet<String>,
    ) -> Self {
        Self {
            lease,
            metadata,
            registered: lease
                .snapshot()
                .catalog
                .kernel_definitions(Some(requested))
                .into_iter()
                .map(|definition| {
                    (
                        definition.name.clone(),
                        (
                            crate::ProviderToolDefinition {
                                name: definition.name,
                                description: definition.description,
                                input_schema: definition.input_schema,
                            },
                            definition.required_permission,
                        ),
                    )
                })
                .collect(),
            descriptors: metadata
                .descriptors
                .iter()
                .filter(|descriptor| requested.contains(&descriptor.canonical_id))
                .map(|descriptor| (descriptor.canonical_id.as_str(), descriptor))
                .collect(),
        }
    }

    pub(crate) fn definitions(&self) -> impl Iterator<Item = &crate::ProviderToolDefinition> {
        self.registered.values().map(|(definition, _)| definition)
    }

    pub(crate) fn registered(&self, definition: &crate::ProviderToolDefinition) -> bool {
        self.registered
            .get(&definition.name)
            .is_some_and(|(original, _)| {
                original.input_schema == definition.input_schema
                    && original.description == definition.description
            })
    }

    pub(crate) fn discover(
        &self,
        definition: &crate::ProviderToolDefinition,
        requested: &std::collections::BTreeSet<String>,
        permission_ceiling: harness_contract::tool::ToolPermissionMode,
        scopes: Option<&[String]>,
    ) -> Option<harness_contract::tool::ToolDescriptorRef> {
        use harness_contract::tool::ToolDescriptorHealth;

        let tool_name = &definition.name;
        let lease = self.lease;
        let metadata = self.metadata;
        let catalog = &lease.snapshot().catalog;
        if !requested.contains(tool_name)
            || catalog.canonical_name(tool_name).as_ref() != Some(tool_name)
            || metadata.catalog_revision != lease.revision()
        {
            return None;
        }
        let (registered, floor) = self.registered.get(tool_name)?;
        if registered.input_schema != definition.input_schema
            || registered.description != definition.description
        {
            return None;
        }
        let descriptor = *self.descriptors.get(tool_name.as_str())?;
        if descriptor.health != ToolDescriptorHealth::Healthy
            || !permission_ceiling.permits(*floor)
            || !permission_ceiling.permits(descriptor.required_permission)
        {
            return None;
        }
        let resolver = catalog.effect_resolver(tool_name);
        let family = matches!(
            (tool_name.as_str(), resolver.resolver_id.as_str()),
            ("read_file", "builtin.readonly")
                | ("tool_orchestrate", "builtin.orchestrate")
                | ("edit", "runtime.action_edit")
        ) && resolver == tools::builtin_effect_resolver_spec(tool_name)
            && descriptor.source == "builtin";
        if family {
            let eligible = scopes.is_none_or(|scopes| {
                scopes.iter().any(|scope| {
                    if tool_name == "tool_orchestrate" && scope == "network:*" {
                        return true;
                    }
                    scope.split_once(':').is_some_and(|(mode, target)| {
                        matches!(mode, "read" | "write" | "workspace")
                            && !target.trim().is_empty()
                            && crate::governed_tool_plan::validate_relative_scope_path(
                                &target.trim().replace('\\', "/"),
                            )
                            .is_ok()
                    })
                })
            });
            return eligible.then(|| descriptor.clone());
        }
        let effect = lease.describe_effect(tool_name, &serde_json::json!({}));
        let control = harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
            .contains(&tool_name.as_str())
            || matches!(
                tool_name.as_str(),
                "context_retrieve"
                    | "working_context"
                    | "private_note"
                    | "evidence_retrieve"
                    | "artifact_publish"
            );
        (permission_ceiling.permits(effect.required_permission)
            && delegated_tool_effect_is_bounded(&effect)
            && (control
                || scopes.is_none_or(|scopes| {
                    effect_has_resource_lease(&effect, scopes, lexical_resource_path_is_authorized)
                })))
        .then(|| descriptor.clone())
    }
}

#[cfg(test)]
pub(crate) fn discover_delegated_tool(
    lease: &tools::ToolHostLease,
    metadata: &harness_contract::tool::ToolDiscoveryReceipt,
    definition: &crate::ProviderToolDefinition,
    requested: &std::collections::BTreeSet<String>,
    permission_ceiling: harness_contract::tool::ToolPermissionMode,
    scopes: Option<&[String]>,
) -> Option<harness_contract::tool::ToolDescriptorRef> {
    DelegatedCatalogProjection::new(lease, metadata, requested).discover(
        definition,
        requested,
        permission_ceiling,
        scopes,
    )
}

#[cfg(test)]
mod resource_projection_tests {
    use super::*;

    #[test]
    fn process_frame_projection_preserves_each_mutation_and_network_grant() {
        let root = tempfile::tempdir().unwrap();
        let lease = tools::ToolHost::builtin("process-frame-contract", root.path()).pin_snapshot();
        let input = serde_json::json!({"steps":[
            {"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"target.txt","content":"x"}},
            {"id":"compute","op":"compute","language":"python","code":"print(1)"}
        ]});
        let effect = lease.describe_effect("tool_orchestrate", &input);
        assert!(effect.spawns_process);
        assert!(!effect.uses_network);
        projection(
            &effect,
            &[
                (".", false),
                (".cowd/checkpoints", true),
                ("target.txt", false),
                ("target.txt", true),
            ],
        );
        let projected = crate::governed_tool_plan::resource_scope_from_effect(&effect);
        assert_eq!(projected.kind, "workspace");
        assert_eq!(projected.paths, vec!["."]);
        assert!(!projected.network && !projected.unknown);
        let grants = ["read:.", "write:target.txt", "write:.cowd/checkpoints"];
        assert!(permits(&effect, &grants));
        for missing in 0..grants.len() {
            let mut narrow = grants.to_vec();
            narrow.remove(missing);
            assert!(!permits(&effect, &narrow), "missing {}", grants[missing]);
        }
        assert!(!permits(
            &effect,
            &[
                "read:target.txt",
                "write:target.txt",
                "write:.cowd/checkpoints"
            ]
        ));
        let mixed = lease.describe_effect("tool_orchestrate", &serde_json::json!({"steps":[
            {"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"target.txt","content":"x"}},
            {"id":"compute","op":"compute","language":"python","code":"print(1)"},
            {"id":"fetch","op":"call","tool":"web_fetch","input":{"url":"https://example.invalid","prompt":"metadata only"}}
        ]}));
        assert!(mixed.spawns_process && mixed.uses_network);
        let projected = crate::governed_tool_plan::resource_scope_from_effect(&mixed);
        assert_eq!(projected.kind, "workspace");
        assert_eq!(projected.paths, vec!["."]);
        assert!(projected.network && !projected.unknown);
        projection(
            &mixed,
            &[
                (".", false),
                (".cowd/checkpoints", true),
                ("target.txt", false),
                ("target.txt", true),
            ],
        );
        let covers = |descriptor: &ToolEffectDescriptor, grants: &[&str]| {
            let grants = grants
                .iter()
                .map(|grant| grant.to_string())
                .collect::<Vec<_>>();
            effect_resource_grants_cover(descriptor, &grants, lexical_resource_path_is_authorized)
        };
        let complete = [
            "read:.",
            "write:target.txt",
            "write:.cowd/checkpoints",
            "network:*",
        ];
        assert!(covers(&mixed, &complete));
        assert!(!delegated_tool_effect_is_bounded(&mixed));
        assert!(!permits(&mixed, &complete));
        assert!(!permits(&mixed, &grants));
        for missing in 0..complete.len() {
            let mut narrow = complete.to_vec();
            narrow.remove(missing);
            assert!(!covers(&mixed, &narrow), "missing {}", complete[missing]);
            assert!(!permits(&mixed, &narrow));
        }
        for wrong in [
            vec![
                "read:target.txt",
                "write:target.txt",
                "write:.cowd/checkpoints",
                "network:*",
            ],
            vec![
                "read:.",
                "write:target.txt-sibling",
                "write:.cowd/checkpoints",
                "network:*",
            ],
            vec![
                "read:.",
                "read:target.txt",
                "write:.cowd/checkpoints",
                "network:*",
            ],
            vec![
                "read:.",
                "write:target.txt",
                "read:.cowd/checkpoints",
                "network:*",
            ],
            vec![
                "read:.",
                "write:target.txt",
                "write:.cowd/checkpoints-sibling",
                "network:*",
            ],
            vec![
                "read:.",
                "write:target.txt",
                "write:.cowd/checkpoints",
                "network:example.invalid",
            ],
        ] {
            assert!(!covers(&mixed, &wrong));
            assert!(!permits(&mixed, &wrong));
        }
        let network = lease.describe_effect("tool_orchestrate", &serde_json::json!({"steps":[
            {"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"target.txt","content":"x"}},
            {"id":"fetch","op":"call","tool":"web_fetch","input":{"url":"https://example.invalid","prompt":"metadata only"}}
        ]}));
        assert!(network.uses_network && !network.spawns_process);
        assert!(delegated_tool_effect_is_bounded(&network));
        let projected = crate::governed_tool_plan::resource_scope_from_effect(&network);
        assert_eq!(projected.kind, "paths");
        assert_eq!(projected.paths, vec![".cowd/checkpoints", "target.txt"]);
        assert!(projected.network && !projected.unknown);
        projection(
            &network,
            &[
                (".cowd/checkpoints", true),
                ("target.txt", false),
                ("target.txt", true),
            ],
        );
        let network_grants = ["write:target.txt", "write:.cowd/checkpoints", "network:*"];
        assert!(covers(&network, &network_grants));
        assert!(permits(&network, &network_grants));
        for missing in 0..network_grants.len() {
            let mut narrow = network_grants.to_vec();
            narrow.remove(missing);
            assert!(
                !covers(&network, &narrow),
                "missing {}",
                network_grants[missing]
            );
            assert!(!permits(&network, &narrow));
        }
        let mut unsupported = network.clone();
        unsupported.scopes.push(scope(
            PermissionResource::File,
            PermissionOperation::Delete,
            Some("target.txt"),
        ));
        assert!(!covers(&unsupported, &complete));
        assert!(!permits(&unsupported, &complete));
    }
    use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
    use harness_contract::tool::{ToolEffectDescriptor, ToolEffectKind, ToolPermissionMode};

    fn scope(
        resource: PermissionResource,
        operation: PermissionOperation,
        target: Option<&str>,
    ) -> PermissionScope {
        PermissionScope {
            resource,
            operation,
            target: target.map(str::to_string),
        }
    }

    fn permits(descriptor: &ToolEffectDescriptor, allowed: &[&str]) -> bool {
        let allowed = allowed
            .iter()
            .map(|scope| scope.to_string())
            .collect::<Vec<_>>();
        effect_has_resource_lease(descriptor, &allowed, lexical_resource_path_is_authorized)
    }

    fn projection(descriptor: &ToolEffectDescriptor, expected: &[(&str, bool)]) {
        let expected = expected
            .iter()
            .map(|(path, write)| (path.to_string(), *write))
            .collect::<Vec<_>>();
        assert_eq!(effect_file_accesses(descriptor), expected);
        let mut evidence = expected
            .iter()
            .map(|(path, write)| format!("{}:{path}", if *write { "write" } else { "read" }))
            .collect::<Vec<_>>();
        if descriptor.uses_network {
            evidence.push("network:*".to_string());
        }
        evidence.sort();
        evidence.dedup();
        assert_eq!(effect_resource_evidence(descriptor), evidence);
    }

    #[test]
    fn r1_composite_discovery_pinned_catalog_does_not_authorize_unknown_calls() {
        let directory = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("pinned-discovery-contract", directory.path());
        let lease = host.pin_snapshot();
        let metadata = lease.catalog_receipt();
        let registered = lease
            .snapshot()
            .catalog
            .kernel_definitions(None)
            .into_iter()
            .find(|definition| definition.name == "read_file")
            .unwrap();
        let definition = crate::ProviderToolDefinition {
            name: registered.name,
            description: registered.description,
            input_schema: registered.input_schema,
        };
        let requested = std::collections::BTreeSet::from([definition.name.clone()]);
        let scopes = vec!["read:.".to_string()];
        assert!(discover_delegated_tool(
            &lease,
            &metadata,
            &definition,
            &requested,
            ToolPermissionMode::ReadOnly,
            Some(&scopes)
        )
        .is_some());
        let mut forged = definition.clone();
        forged.name = "unregistered_discovery_call".to_string();
        let mut forged_metadata = metadata.clone();
        let mut descriptor = metadata
            .descriptors
            .iter()
            .find(|descriptor| descriptor.canonical_id == definition.name)
            .unwrap()
            .clone();
        descriptor.canonical_id = forged.name.clone();
        descriptor.required_permission = ToolPermissionMode::ReadOnly;
        descriptor.health = harness_contract::tool::ToolDescriptorHealth::Healthy;
        forged_metadata.descriptors.push(descriptor);
        let forged_requested = std::collections::BTreeSet::from([forged.name.clone()]);
        assert!(discover_delegated_tool(
            &lease,
            &forged_metadata,
            &forged,
            &forged_requested,
            ToolPermissionMode::DangerFullAccess,
            Some(&scopes)
        )
        .is_none());
        assert_eq!(
            lease
                .describe_effect(&forged.name, &serde_json::json!({}))
                .effect_kind,
            ToolEffectKind::Unknown
        );
        let mut stale = metadata.clone();
        stale.catalog_revision = stale.catalog_revision.checked_add(1).unwrap();
        assert!(discover_delegated_tool(
            &lease,
            &stale,
            &definition,
            &requested,
            ToolPermissionMode::ReadOnly,
            Some(&scopes)
        )
        .is_none());
        let mut missing = metadata.clone();
        missing
            .descriptors
            .retain(|descriptor| descriptor.canonical_id != definition.name);
        assert!(discover_delegated_tool(
            &lease,
            &missing,
            &definition,
            &requested,
            ToolPermissionMode::ReadOnly,
            Some(&scopes)
        )
        .is_none());
        assert!(discover_delegated_tool(
            &lease,
            &metadata,
            &definition,
            &std::collections::BTreeSet::new(),
            ToolPermissionMode::ReadOnly,
            Some(&scopes)
        )
        .is_none());
        let mut changed = definition.clone();
        changed.input_schema = serde_json::json!({"type":"string"});
        assert!(discover_delegated_tool(
            &lease,
            &metadata,
            &changed,
            &requested,
            ToolPermissionMode::ReadOnly,
            Some(&scopes)
        )
        .is_none());
        assert_eq!(host.pin_snapshot().revision(), lease.revision());
    }

    #[test]
    fn r1_composite_discovery_edit_family_keeps_concrete_checkpoint_authority() {
        let directory = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("edit-discovery-contract", directory.path())
            .with_authorization_lease_verifier(std::sync::Arc::new(
                crate::AuthorizationNegotiator::verify_lease_signature,
            ));
        let lease = host.pin_snapshot();
        let metadata = lease.catalog_receipt();
        let definition = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .find(|definition| definition.name == "edit")
            .unwrap();
        let definition = crate::ProviderToolDefinition {
            name: definition.name,
            description: definition.description,
            input_schema: definition.input_schema,
        };
        let requested = std::collections::BTreeSet::from(["edit".to_string()]);
        let discover = |receipt: &harness_contract::tool::ToolDiscoveryReceipt,
                        definition: &crate::ProviderToolDefinition,
                        ceiling,
                        allowed: &[&str]| {
            discover_delegated_tool(
                &lease,
                receipt,
                definition,
                &requested,
                ceiling,
                Some(
                    &allowed
                        .iter()
                        .map(|scope| scope.to_string())
                        .collect::<Vec<_>>(),
                ),
            )
        };
        let grants = ["read:.", "write:out/a", "write:.cowd/checkpoints"];
        assert!(discover(
            &metadata,
            &definition,
            ToolPermissionMode::WorkspaceWrite,
            &grants
        )
        .is_some());
        assert!(discover(
            &metadata,
            &definition,
            ToolPermissionMode::WorkspaceWrite,
            &["read:.", "write:out/a"]
        )
        .is_some());
        assert!(discover(
            &metadata,
            &definition,
            ToolPermissionMode::ReadOnly,
            &grants
        )
        .is_none());
        for allowed in [
            vec![],
            vec!["write:../escape"],
            vec!["write:/absolute"],
            vec!["write:"],
            vec!["network:*"],
        ] {
            assert!(discover(
                &metadata,
                &definition,
                ToolPermissionMode::WorkspaceWrite,
                &allowed
            )
            .is_none());
        }
        for mutation in 0..5 {
            let mut altered = metadata.clone();
            let descriptor = altered
                .descriptors
                .iter_mut()
                .find(|descriptor| descriptor.canonical_id == "edit")
                .unwrap();
            match mutation {
                0 => descriptor.source = "custom".into(),
                1 => descriptor.health = harness_contract::tool::ToolDescriptorHealth::Unavailable,
                2 => descriptor.required_permission = ToolPermissionMode::DangerFullAccess,
                3 => descriptor.canonical_id = "shadow_edit".into(),
                _ => altered.catalog_revision += 1,
            }
            assert!(discover(
                &altered,
                &definition,
                ToolPermissionMode::WorkspaceWrite,
                &grants
            )
            .is_none());
        }
        for mutation in 0..3 {
            let mut altered = definition.clone();
            match mutation {
                0 => altered.name = "write_file".into(),
                1 => altered.description = Some("shadow".into()),
                _ => altered.input_schema = serde_json::json!({"type":"string"}),
            }
            assert!(discover(
                &metadata,
                &altered,
                ToolPermissionMode::WorkspaceWrite,
                &grants
            )
            .is_none());
        }
        let input = serde_json::json!({"action":"write","path":"out/a","content":"actual target"});
        let concrete = lease.describe_effect("edit", &input);
        assert_eq!(
            concrete.assessment.data_sensitivity,
            harness_contract::policy::DataClassification::Internal
        );
        assert_eq!(
            tools::builtin_effect_resolver_spec("edit").resolver_version,
            3
        );
        assert_eq!(
            crate::AuthorizationNegotiator::compile_effective_descriptor(
                &concrete,
                &input.to_string()
            )
            .descriptor
            .required_permission,
            ToolPermissionMode::WorkspaceWrite
        );
        projection(
            &concrete,
            &[
                (".cowd/checkpoints", true),
                ("out/a", false),
                ("out/a", true),
            ],
        );
        for target in [".env", ".ssh/id_rsa", "credentials.json"] {
            let sensitive = lease.describe_effect(
                "edit",
                &serde_json::json!({"action":"write","path":target,"content":"secret"}),
            );
            assert_eq!(
                sensitive.assessment.data_sensitivity,
                harness_contract::policy::DataClassification::Secret
            );
            assert_eq!(
                crate::AuthorizationNegotiator::compile_effective_descriptor(&sensitive, "{}")
                    .descriptor
                    .required_permission,
                ToolPermissionMode::DangerFullAccess
            );
        }
        const SDK2_EFFECT: &str = r###"{
    "approval_class": "policy",
    "assessment": {
      "blast_radius": "workspace",
      "data_sensitivity": "secret",
      "externality": "workspace",
      "novelty": "routine",
      "reversibility": "compensatable"
    },
    "descriptor_hash": "a556282fb072b286",
    "effect_kind": "write",
    "idempotency": "idempotent_with_key",
    "mutates_packages": false,
    "mutates_system": false,
    "required_permission": "workspace-write",
    "scopes": [
      {
        "operation": "read",
        "resource": "file",
        "target": null
      },
      {
        "operation": "write",
        "resource": "file",
        "target": ".cowd/checkpoints"
      },
      {
        "operation": "write",
        "resource": "file",
        "target": "out/a"
      }
    ],
    "spawns_process": false,
    "tool_id": "edit",
    "uses_network": false
  }"###;
        const SDK2_INPUT: &str = r###"{
    "action": "write",
    "content": "captured-v2-edit",
    "path": "out/a"
  }"###;
        let historical: ToolEffectDescriptor = serde_json::from_str(SDK2_EFFECT).unwrap();
        let historical_input: serde_json::Value = serde_json::from_str(SDK2_INPUT).unwrap();
        assert_eq!(historical.descriptor_hash, "a556282fb072b286");
        assert_eq!(historical.effect_kind, ToolEffectKind::Write);
        assert_eq!(
            historical.assessment.data_sensitivity,
            harness_contract::policy::DataClassification::Secret
        );
        projection(
            &historical,
            &[(".", false), (".cowd/checkpoints", true), ("out/a", true)],
        );
        let narrow = ["read:out/a", "write:out/a", "write:.cowd/checkpoints"];
        assert!(!permits(&historical, &narrow));
        assert!(!permits(
            &historical,
            &["write:out/a", "write:.cowd/checkpoints"]
        ));
        let current = lease.describe_effect("edit", &historical_input);
        assert_ne!(current.descriptor_hash, historical.descriptor_hash);
        assert!(permits(&current, &narrow));
        assert!(permits(
            &current,
            &["write:out/a", "write:.cowd/checkpoints"]
        ));
        let historical_effective = crate::AuthorizationNegotiator::compile_effective_descriptor(
            &historical,
            &historical_input.to_string(),
        );
        assert_eq!(
            historical_effective.descriptor.required_permission,
            ToolPermissionMode::DangerFullAccess
        );
        let request = crate::AuthorizationRequest {
            principal_id: "sdk3-consumer".into(),
            capability: "edit".into(),
            input: historical_input.to_string(),
            idempotency_key: "sdk3-consumer".into(),
            effect: current,
            parent_ceiling: ToolPermissionMode::WorkspaceWrite,
            parent_lease_id: None,
            policy_revision: 1,
            recovery_scope: "sdk3-consumer".into(),
            context: crate::PermissionContext::default(),
            safe_alternatives: vec![],
        };
        let evaluated = crate::AuthorizationNegotiator::new().assess_effective(
            &crate::permissions::PermissionPolicy::new(ToolPermissionMode::WorkspaceWrite),
            &request,
        );
        let signed = evaluated
            .assessment
            .lease
            .clone()
            .expect("current v3 exact edit receives its own signed lease");
        assert!(crate::AuthorizationNegotiator::verify_lease_signature(
            &signed
        ));
        let authorized = crate::ToolPolicy
            .authorize(
                &evaluated.effective,
                &evaluated.assessment,
                &request.idempotency_key,
                signed.clone(),
                30,
            )
            .unwrap();
        assert!(lease
            .validate_authorization(&authorized.authorization, "edit", &historical_input)
            .is_ok());
        let mut forged = authorized.authorization.clone();
        forged.authorization_lease.signature.push_str("-forged");
        assert_eq!(
            lease
                .validate_authorization(&forged, "edit", &historical_input)
                .unwrap_err(),
            tools::ToolHostError::InvalidLeaseSignature
        );
        let error = crate::ToolPolicy
            .authorize(
                &historical_effective,
                &evaluated.assessment,
                &request.idempotency_key,
                signed,
                30,
            )
            .unwrap_err()
            .to_string();
        assert_eq!(error, "authorization lease does not permit required mode");
        assert_eq!(historical.descriptor_hash, "a556282fb072b286");
        assert!(permits(&concrete, &grants));
        assert!(permits(
            &concrete,
            &["write:out/a", "write:.cowd/checkpoints"]
        ));
        for allowed in [
            vec!["read:.", "write:out/a"],
            vec!["read:.", "write:.cowd/checkpoints"],
            vec!["read:.", "write:out/b", "write:.cowd/checkpoints"],
        ] {
            assert!(!permits(&concrete, &allowed));
        }
        for target in ["out/b", ".", "../escape", "/tmp/escape", ".cowd/outside"] {
            let effect = lease.describe_effect(
                "edit",
                &serde_json::json!({"action":"write","path":target,"content":"no"}),
            );
            assert!(!permits(&effect, &grants), "{target}");
        }
        let sibling = lease.describe_effect(
            "edit",
            &serde_json::json!({"action":"write","path":"out/b","content":"other"}),
        );
        assert!(effect_file_accesses(&concrete).contains(&(".cowd/checkpoints".into(), true)));
        assert!(
            effect_file_accesses(&sibling).contains(&(".cowd/checkpoints".into(), true)),
            "distinct targets still overlap on checkpoint authority"
        );
        let custom = tools::RuntimeToolDefinition {
            name: "custom_edit".into(),
            description: Some("not builtin".into()),
            input_schema: serde_json::json!({"type":"object"}),
            required_permission: ToolPermissionMode::WorkspaceWrite,
            effect_resolver: tools::builtin_effect_resolver_spec("edit"),
        };
        let mut shadow = custom.clone();
        shadow.name = "edit".into();
        assert!(tools::ToolCatalog::builtin()
            .with_runtime_tools(vec![shadow])
            .is_err());
        let catalog = tools::ToolCatalog::builtin()
            .with_runtime_tools(vec![custom])
            .unwrap();
        let custom_host = tools::ToolHost::new(
            "custom-edit",
            directory.path(),
            tools::ToolHostSnapshot::new(
                std::sync::Arc::new(catalog),
                std::sync::Arc::new(tools::lsp_client::LspRegistry::default()),
                None,
            ),
        );
        let custom_lease = custom_host.pin_snapshot();
        let custom_metadata = custom_lease.catalog_receipt();
        let custom_definition = custom_lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .find(|definition| definition.name == "custom_edit")
            .unwrap();
        assert!(discover_delegated_tool(
            &custom_lease,
            &custom_metadata,
            &crate::ProviderToolDefinition {
                name: custom_definition.name,
                description: custom_definition.description,
                input_schema: custom_definition.input_schema,
            },
            &std::collections::BTreeSet::from(["custom_edit".into()]),
            ToolPermissionMode::WorkspaceWrite,
            Some(
                &grants
                    .iter()
                    .map(|scope| scope.to_string())
                    .collect::<Vec<_>>()
            )
        )
        .is_none());
    }

    #[test]
    fn r1_resource_projection_preserves_typed_nonfile_and_file_boundaries() {
        let workspace = std::env::current_dir().unwrap();
        let lease =
            tools::ToolHost::builtin("typed-resource-projection", &workspace).pin_snapshot();
        let metadata = lease.describe_effect("tool_search", &serde_json::json!({"query":"read"}));
        assert_eq!(metadata.effect_kind, ToolEffectKind::Read);
        assert!(delegated_tool_effect_is_bounded(&metadata));
        projection(&metadata, &[]);
        assert!(permits(&metadata, &[]));
        assert!(permits(&metadata, &["read:src/a"]));

        for resource in [PermissionResource::Tool, PermissionResource::Session] {
            for target in [Some("tool_search"), Some("secret.txt"), None, Some("")] {
                let mut descriptor = metadata.clone();
                descriptor.scopes =
                    vec![scope(resource.clone(), PermissionOperation::Read, target)];
                projection(&descriptor, &[]);
                for allowed in [vec![], vec!["read:src/a".to_string()]] {
                    assert!(effect_has_resource_lease(
                        &descriptor,
                        &allowed,
                        |_, _, _| {
                            panic!("non-File metadata must not request a file authorization")
                        }
                    ));
                }
            }
        }

        let mut mixed = metadata.clone();
        mixed.scopes = vec![
            scope(
                PermissionResource::Tool,
                PermissionOperation::Read,
                Some("tool_search"),
            ),
            scope(
                PermissionResource::Session,
                PermissionOperation::Read,
                Some("secret.txt"),
            ),
            scope(
                PermissionResource::File,
                PermissionOperation::Read,
                Some("src/a"),
            ),
            scope(
                PermissionResource::File,
                PermissionOperation::Write,
                Some("out/b"),
            ),
            scope(
                PermissionResource::File,
                PermissionOperation::Read,
                Some("src/a"),
            ),
        ];
        projection(&mixed, &[("out/b", true), ("src/a", false)]);
        assert!(permits(&mixed, &["read:src/a", "write:out/b"]));
        for allowed in [
            vec![],
            vec!["read:src/a"],
            vec!["write:out/b"],
            vec!["read:src/a", "read:out/b"],
            vec!["read:src/ab", "write:out/b"],
            vec!["read:src/a", "write:out/bb"],
        ] {
            assert!(!permits(&mixed, &allowed));
        }

        for target in [None, Some(""), Some(" \t ")] {
            for (operation, write) in [
                (PermissionOperation::Read, false),
                (PermissionOperation::Write, true),
            ] {
                let mut descriptor = metadata.clone();
                descriptor.scopes = vec![scope(PermissionResource::File, operation, target)];
                projection(&descriptor, &[(".", write)]);
                for allowed in [
                    vec![],
                    vec!["read:src/a"],
                    vec!["write:out/b"],
                    vec!["network:*"],
                ] {
                    assert!(!permits(&descriptor, &allowed));
                }
                assert_eq!(permits(&descriptor, &["read:."]), !write);
                for whole in ["write:.", "workspace:.", "write:./", "workspace:./"] {
                    assert!(permits(&descriptor, &[whole]));
                }
            }
        }
        for operation in [
            PermissionOperation::Delete,
            PermissionOperation::Execute,
            PermissionOperation::Send,
            PermissionOperation::Control,
            PermissionOperation::Call,
        ] {
            for target in [None, Some("src/a")] {
                let mut descriptor = metadata.clone();
                descriptor.scopes =
                    vec![scope(PermissionResource::File, operation.clone(), target)];
                projection(&descriptor, &[(target.unwrap_or("."), true)]);
                assert!(!permits(&descriptor, &["workspace:.", "network:*"]));
            }
        }

        let mut network = metadata.clone();
        network.uses_network = true;
        projection(&network, &[]);
        assert!(!permits(&network, &[]));
        assert!(!permits(&network, &["read:."]));
        assert!(permits(&network, &["network:*"]));
        network.scopes.push(scope(
            PermissionResource::File,
            PermissionOperation::Read,
            Some("src/a"),
        ));
        projection(&network, &[("src/a", false)]);
        for file_grant in [false, true] {
            for network_grant in [false, true] {
                let mut allowed = Vec::new();
                if file_grant {
                    allowed.push("read:src/a");
                }
                if network_grant {
                    allowed.push("network:*");
                }
                assert_eq!(permits(&network, &allowed), file_grant && network_grant);
            }
        }

        let mut process = lease.describe_effect(
            "execute_code",
            &serde_json::json!({
                "language":"python", "code":"print(1)"
            }),
        );
        assert!(process.spawns_process);
        assert!(delegated_tool_effect_is_bounded(&process));
        process
            .scopes
            .retain(|scope| scope.resource != PermissionResource::File);
        projection(&process, &[(".", false)]);
        for allowed in [vec![], vec!["read:src/a"], vec!["network:*"]] {
            assert!(!permits(&process, &allowed));
        }
        assert!(permits(&process, &["read:."]));
        process.scopes.push(scope(
            PermissionResource::File,
            PermissionOperation::Write,
            Some("out/b"),
        ));
        projection(&process, &[(".", false), ("out/b", true)]);
        assert!(permits(&process, &["read:.", "write:out/b"]));
        assert!(!permits(&process, &["read:."]));
        assert!(!permits(&process, &["write:out/b"]));
        assert!(!permits(&process, &["read:.", "read:out/b"]));
        let mut unbounded = process.clone();
        unbounded.required_permission = ToolPermissionMode::DangerFullAccess;
        assert!(!permits(&unbounded, &["workspace:.", "network:*"]));
        unbounded = process.clone();
        unbounded.uses_network = true;
        assert!(!permits(&unbounded, &["workspace:.", "network:*"]));

        let unknown = lease.describe_effect(
            "unregistered_resource_projection_tool",
            &serde_json::json!({}),
        );
        assert_eq!(unknown.effect_kind, ToolEffectKind::Unknown);
        assert!(lease
            .catalog_receipt()
            .descriptors
            .iter()
            .all(|descriptor| descriptor.canonical_id != "unregistered_resource_projection_tool"));
        projection(&unknown, &[(".", false)]);
        assert!(!permits(&unknown, &["workspace:.", "network:*"]));
        for scopes in [
            vec![],
            vec![scope(
                PermissionResource::Tool,
                PermissionOperation::Read,
                Some("secret.txt"),
            )],
            vec![scope(
                PermissionResource::File,
                PermissionOperation::Read,
                Some("src/a"),
            )],
        ] {
            let mut descriptor = unknown.clone();
            descriptor.spawns_process = false;
            descriptor.uses_network = false;
            descriptor.scopes = scopes;
            let expected = if descriptor
                .scopes
                .iter()
                .any(|scope| scope.resource == PermissionResource::File)
            {
                vec![("src/a", false)]
            } else {
                vec![]
            };
            projection(&descriptor, &expected);
            assert!(!permits(&descriptor, &[]));
            assert!(!permits(&descriptor, &["workspace:.", "network:*"]));
        }
        for kind in [
            ToolEffectKind::Package,
            ToolEffectKind::System,
            ToolEffectKind::Destructive,
        ] {
            let mut descriptor = metadata.clone();
            descriptor.scopes.clear();
            descriptor.effect_kind = kind;
            projection(&descriptor, &[]);
            assert!(!permits(&descriptor, &["workspace:.", "network:*"]));
        }
        for (packages, system) in [(true, false), (false, true), (true, true)] {
            let mut descriptor = metadata.clone();
            descriptor.scopes.clear();
            descriptor.mutates_packages = packages;
            descriptor.mutates_system = system;
            projection(&descriptor, &[]);
            assert!(!permits(&descriptor, &["workspace:.", "network:*"]));
        }

        let mut plugin = metadata.clone();
        plugin.tool_id = "plugin.resource_projection".to_string();
        plugin.scopes.clear();
        projection(&plugin, &[]);
        assert!(permits(&plugin, &[]));
        plugin.scopes.push(scope(
            PermissionResource::Tool,
            PermissionOperation::Read,
            Some("secret.txt"),
        ));
        projection(&plugin, &[]);
        assert!(permits(&plugin, &[]));
        plugin.scopes.push(scope(
            PermissionResource::File,
            PermissionOperation::Write,
            Some("out/b"),
        ));
        projection(&plugin, &[("out/b", true)]);
        assert!(!permits(&plugin, &[]));
        assert!(!permits(&plugin, &["read:out/b"]));
        assert!(!permits(&plugin, &["write:out/bb"]));
        assert!(permits(&plugin, &["write:out/b"]));
        assert!(!permits(&mixed, &[]));
        assert!(!permits(&process, &[]));
        assert!(!permits(&network, &[]));
    }
}
