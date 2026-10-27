//! Governed tool-wave execution, evidence preparation, and result publication.

use super::*;
use std::sync::Mutex;

/// Construct each responsibility future outside its caller's poll frame. The
/// factory only allocates; polling stays in the original task and cancel scope.
#[inline(never)]
fn boxed_tool_future<'future, F>(
    build: impl FnOnce() -> F,
) -> GovernedToolFuture<'future, F::Output>
where
    F: std::future::Future + Send + 'future,
{
    Box::pin(build())
}

// Derive the checkpoint's real read range from the original prepared Step,
// after replay removal. No guessed parent directory or permission downgrade.
fn checkpoint_paths_for_admission(
    plan: &crate::GovernedToolPlan,
    current: Option<tools::ToolHostLease>,
) -> Result<Vec<String>, RuntimeError> {
    use harness_contract::goal::ProofEffectPurpose;
    use harness_contract::policy::{PermissionOperation, PermissionResource};
    use harness_contract::tool::ToolEffectKind;
    use std::path::{Component, Path};
    let mut paths = BTreeSet::new();
    for task in &plan.tasks {
        let effect = &task.invocation.effect;
        if effect.effect_kind == ToolEffectKind::Read
            && !effect.spawns_process
            && !effect.mutates_packages
            && !effect.mutates_system
        {
            continue;
        }
        if effect.effect_kind != ToolEffectKind::Write
            || effect.uses_network
            || effect.spawns_process
            || effect.mutates_packages
            || effect.mutates_system
            || task.effect != *effect
        {
            return Err(RuntimeError::new(
                "checkpoint cannot cover an unknown or non-file effect",
            ));
        }
        let binding = task
            .step_binding
            .as_ref()
            .ok_or_else(|| RuntimeError::new("checkpoint requires the original prepared Step"))?;
        let step = binding.resolve(current.clone())?;
        let lease = step.lease();
        let resolution = lease.describe_effect_resolution(&task.tool_name, &task.normalized_input);
        if resolution.descriptor != *effect {
            return Err(RuntimeError::new("checkpoint prepared descriptor changed"));
        }
        let mut has_write = false;
        let mut task_paths = BTreeSet::new();
        for entry in &resolution.scope_purposes {
            let scope = &entry.declared_scope;
            if entry.purpose == ProofEffectPurpose::AutoCheckpointMaintenance {
                continue;
            }
            if scope.resource == PermissionResource::Tool {
                continue; // the capability coordinate is not a filesystem range
            }
            if scope.resource != PermissionResource::File
                || !matches!(
                    scope.operation,
                    PermissionOperation::Read | PermissionOperation::Write
                )
            {
                return Err(RuntimeError::new(
                    "checkpoint cannot cover a non-file scope",
                ));
            }
            let raw = scope
                .target
                .as_deref()
                .ok_or_else(|| RuntimeError::new("checkpoint file scope is unbounded"))?;
            let path = Path::new(raw);
            let relative = if path.is_absolute() {
                path.strip_prefix(lease.workspace_root())
                    .map_err(|_| RuntimeError::new("checkpoint file scope escapes workspace"))?
            } else {
                path
            };
            let canonical = relative
                .to_str()
                .ok_or_else(|| RuntimeError::new("checkpoint path is not UTF-8"))?;
            if canonical.is_empty()
                || canonical.trim() != canonical
                || canonical.contains('\\')
                || relative
                    .components()
                    .any(|c| !matches!(c, Component::Normal(part) if part != ".cowd"))
                || relative
                    .components()
                    .collect::<std::path::PathBuf>()
                    .to_str()
                    != Some(canonical)
            {
                return Err(RuntimeError::new(
                    "checkpoint file scope is not a bounded canonical workspace path",
                ));
            }
            // Original path policy checks existing ancestors/symlinks/escape.
            // The checkpoint executor receives these SAME paths and performs
            // its original strict symlink and recursive-directory checks.
            lease
                .path_policy()
                .resolve(raw)
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            has_write |= scope.operation == PermissionOperation::Write;
            task_paths.insert(canonical.to_owned());
        }
        if !has_write || task_paths.is_empty() {
            return Err(RuntimeError::new(
                "checkpoint has no complete concrete file mutation range",
            ));
        }
        paths.extend(task_paths);
    }
    if paths.is_empty() {
        return Err(RuntimeError::new(
            "checkpoint has no unreplayed concrete file targets",
        ));
    }
    Ok(paths.into_iter().collect())
}

pub(crate) struct RootNodeActivityRuntime<'runtime, C, T> {
    runtime: &'runtime mut ConversationRuntime<C, T>,
    original_bus: Option<Arc<crate::CowdEventBus>>,
}

impl<C, T> std::ops::Deref for RootNodeActivityRuntime<'_, C, T> {
    type Target = ConversationRuntime<C, T>;

    fn deref(&self) -> &Self::Target {
        self.runtime
    }
}

impl<C, T> std::ops::DerefMut for RootNodeActivityRuntime<'_, C, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.runtime
    }
}

impl<C, T> Drop for RootNodeActivityRuntime<'_, C, T> {
    fn drop(&mut self) {
        self.runtime.cowd_bus = self.original_bus.take();
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProgramRawEncoding {
    LegacyOrdinary16,
    BadAnchor(&'static str),
}

#[cfg(test)]
type ProgramRawKey = (usize, String, String);

#[cfg(test)]
fn program_raw_encodings(
) -> &'static std::sync::Mutex<std::collections::BTreeMap<ProgramRawKey, (ProgramRawEncoding, usize)>>
{
    static ENCODINGS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::BTreeMap<ProgramRawKey, (ProgramRawEncoding, usize)>>,
    > = std::sync::OnceLock::new();
    ENCODINGS.get_or_init(Default::default)
}

#[cfg(test)]
pub(crate) struct ProgramRawEncodingGuard {
    key: ProgramRawKey,
    _store: Arc<crate::RuntimeEventStore>,
}

#[cfg(test)]
impl Drop for ProgramRawEncodingGuard {
    fn drop(&mut self) {
        let entry = program_raw_encodings().lock().unwrap().remove(&self.key);
        if !std::thread::panicking() {
            assert_eq!(entry.map(|(_, hits)| hits), Some(1));
        }
    }
}

#[cfg(test)]
impl crate::RuntimeServices {
    pub(crate) fn program_raw_encoding_for_test(
        store: Arc<crate::RuntimeEventStore>,
        session: &str,
        call: &str,
        field: &'static str,
    ) -> ProgramRawEncodingGuard {
        assert_eq!(call, "native-publication-small");
        let key = (
            Arc::as_ptr(&store) as usize,
            session.to_string(),
            call.to_string(),
        );
        let mut entries = program_raw_encodings().lock().unwrap();
        assert!(!entries.contains_key(&key));
        let mode = if field == "legacy" {
            ProgramRawEncoding::LegacyOrdinary16
        } else {
            ProgramRawEncoding::BadAnchor(field)
        };
        entries.insert(key.clone(), (mode, 0));
        ProgramRawEncodingGuard { key, _store: store }
    }
}

#[cfg(test)]
impl<C, T> ConversationRuntime<C, T>
where
    C: ApiClient,
    T: ToolExecutor,
{
    fn apply_raw_origin_encoding_for_test(
        &self,
        record: &ToolInvocationRecord,
        kind: &str,
        payload: &mut serde_json::Value,
    ) {
        if kind != "tool.invocation.completed" {
            return;
        }
        let Some(store) = self.runtime_event_store.as_ref() else {
            return;
        };
        let key = (
            Arc::as_ptr(store) as usize,
            self.session_id().to_string(),
            record.tool_call_id.clone(),
        );
        let mode = {
            let mut entries = program_raw_encodings().lock().unwrap();
            let Some((mode, hits)) = entries.get_mut(&key) else {
                return;
            };
            *hits += 1;
            assert_eq!(*hits, 1);
            *mode
        };
        assert_eq!(record.tool_name, "artifact_publish");
        assert!(payload["runtime_raw_content_v1"].is_object());
        match mode {
            ProgramRawEncoding::LegacyOrdinary16 => {
                payload
                    .as_object_mut()
                    .unwrap()
                    .remove("runtime_raw_content_v1");
            }
            ProgramRawEncoding::BadAnchor(field) => {
                payload["runtime_raw_content_v1"][field] = match field {
                    "version" => serde_json::json!(2),
                    "byte_count" => serde_json::json!(
                        payload["runtime_raw_content_v1"]["byte_count"]
                            .as_u64()
                            .unwrap()
                            + 1
                    ),
                    "tool_call_id" => serde_json::json!("foreign-call"),
                    "ref_id" => serde_json::json!("tool://foreign-raw"),
                    "visibility_scope" => serde_json::json!("session:foreign-session"),
                    "sha256" => serde_json::json!(format!("sha256:{}", "0".repeat(64))),
                    "selector" => serde_json::json!("artifact://missing-program-raw-anchor"),
                    "media_type" => serde_json::json!("application/foreign"),
                    _ => panic!("unapproved raw anchor field"),
                };
            }
        }
    }
}

/// PLAN B-5: minimum tool-result grant for a read, so a spent budget cannot
/// reduce a read to an unusable shell.
const READ_TOOL_RESULT_FLOOR_TOKENS: u64 = 8_192;

/// An agentic action receipt is a protocol object, not a payload to summarize.
///
/// `build_tool_receipt` gives every JSON tool output a prose prefix, which is right
/// for a read - the model needs the reference and a hint - but wrong here: the
/// receipt's `status` / `duplicate` / `changed_refs` / `actionable` fields are what
/// the model acts on, and Runtime's own progress measure parses the very text it
/// hands the model. In one live run 74 of 75 delegated tool-batch observations
/// reported no verified gain for that reason, while the same batches' own summaries
/// reported new coverage.
///
/// Deliver the receipt verbatim exactly when it would be counted as progress, so
/// the measure and the model read one object. Anything that does not parse as an
/// applied receipt keeps the generic prose receipt.
fn agentic_action_body(output: &str) -> Option<String> {
    let observation =
        serde_json::from_str::<harness_contract::agent_action::AgentActionObservation>(output)
            .ok()?;
    (observation.status == harness_contract::agent_action::AgentActionStatus::Applied
        && !observation.duplicate
        && !observation.changed_refs.is_empty())
    .then(|| output.to_string())
}

fn checked_delegated_effect_parent(
    parent: Option<(
        harness_contract::execution_graph::ExecutionParentBinding,
        u32,
    )>,
    run_ref: Option<&str>,
    identity: Option<&harness_contract::execution::ExecutionIdentity>,
    activity: Option<&harness_contract::projection::RuntimeActivityBinding>,
    session_id: &str,
) -> Result<
    Option<(
        harness_contract::execution_graph::ExecutionParentBinding,
        u32,
    )>,
    RuntimeError,
> {
    let Some((binding, attempt)) = parent else {
        if run_ref.is_some()
            || identity.is_some_and(|owner| owner.agent_run_id().is_some())
            || activity.is_some_and(|owner| owner.agent_run_id.is_some())
        {
            return Err(RuntimeError::new(
                "delegated tool effect requires its original packet parent",
            ));
        }
        return Ok(None);
    };
    let identity = identity.ok_or_else(|| {
        RuntimeError::new("delegated tool effect requires its original execution identity")
    })?;
    let run = identity
        .agent_run_id()
        .filter(|run| !run.trim().is_empty())
        .ok_or_else(|| {
            RuntimeError::new("delegated tool effect requires its original Agent run")
        })?;
    if binding.execution_id.trim().is_empty()
        || binding.node_id.trim().is_empty()
        || attempt == 0
        || identity.session_id() != Some(session_id)
        || identity.graph_id() != Some(binding.execution_id.as_str())
        || identity.node_id() != Some(binding.node_id.as_str())
        || run_ref != Some(run)
        || activity.is_some_and(|owner| {
            owner.session_id != session_id
                || owner.agent_run_id.as_deref() != Some(run)
                || owner
                    .node_id
                    .as_ref()
                    .is_some_and(|node| node != &binding.node_id)
        })
    {
        return Err(RuntimeError::new(
            "delegated tool effect parent crosses its original Step or activity",
        ));
    }
    Ok(Some((binding, attempt)))
}

fn authenticated_root_effect_owner(
    root: &crate::CowdExecutionContext,
    owner: harness_contract::projection::RuntimeActivityBinding,
    graph: &harness_contract::execution_graph::ExecutionGraph,
) -> Result<(harness_contract::projection::RuntimeActivityBinding, u32), RuntimeError> {
    owner.validate().map_err(RuntimeError::new)?;
    let node_id = owner
        .node_id
        .as_ref()
        .filter(|node_id| !node_id.is_empty())
        .ok_or_else(|| RuntimeError::new("root effect activity has no original node"))?;
    let attempt = graph
        .recovery_cursor
        .node_attempts
        .get(node_id)
        .copied()
        .filter(|attempt| *attempt > 0)
        .ok_or_else(|| RuntimeError::new("root effect activity has no original attempt"))?;
    if graph.id != root.execution_id
        || graph.parent_execution.is_some()
        || owner.root_execution_id != root.execution_id
        || owner.session_id != root.session_id
        || owner.turn_id != root.turn_id
        || owner.agent_run_id.is_some()
        || !graph.lineage.as_ref().is_some_and(|lineage| {
            lineage.session_id == root.session_id
                && lineage.turn_id == root.turn_id
                && lineage.root_task_id == owner.root_task_id
                && lineage.task_id == owner.task_id
                && lineage.generation == owner.generation
        })
        || graph.node_statuses.get(node_id)
            != Some(&harness_contract::execution_graph::ExecutionNodeStatus::Running)
        || !graph.nodes.iter().any(|node| &node.id == node_id)
    {
        return Err(RuntimeError::new(
            "root effect original activity does not own this running node",
        ));
    }
    Ok((owner, attempt))
}

fn original_ticket_start_event(
    store: &crate::RuntimeEventStore,
    start: &crate::DurableRuntimeEvent,
) -> Result<crate::execution_core::graph::ExecutionGraphEvent, RuntimeError> {
    let payload = store
        .resolve_graph_record(start)
        .map_err(RuntimeError::new)?;
    serde_json::from_value(payload).map_err(|error| RuntimeError::new(error.to_string()))
}

struct LockedRootEffectScope {
    scope: harness_contract::goal::ProofEffectScope,
    resolver: crate::path_identity::WorkspacePathIdentityResolver,
    _lease: Option<crate::execution_core::graph::ScopeLockLease>,
}

fn validate_root_physical_scope(
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    scope: &harness_contract::goal::ProofEffectScope,
) -> Result<(), ToolError> {
    for target in &scope.targets {
        let physical = resolver
            .physical_file_identity_at_admission(&target.path)
            .map_err(|error| ToolError::new(format!("admitted physical scope changed: {error}")))?;
        if physical != target.physical_identity || target.alias_checked != physical.is_some() {
            return Err(ToolError::new(
                "admitted physical scope changed before execution",
            ));
        }
    }
    Ok(())
}

async fn lock_root_effect_scope(
    request: &crate::RuntimeToolExecutionRequest,
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
    current: Option<tools::ToolHostLease>,
    locks: &crate::execution_core::graph::ScopeLockManager,
    executor_owns_admission: bool,
) -> Result<LockedRootEffectScope, ToolError> {
    let step = request
        .step_binding
        .as_ref()
        .ok_or_else(|| ToolError::new("effect admission requires the original Step"))?
        .resolve(current.clone())
        .map_err(|error| ToolError::new(error.to_string()))?;
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(
        step.lease().workspace_root(),
    )
    .map_err(|error| ToolError::new(error.to_string()))?;
    let (requests, identities) =
        crate::bound_tool_batch::bound_effect_lock_requests(&resolver, descriptor)?;
    let lease = if executor_owns_admission || requests.is_empty() {
        None
    } else {
        Some(
            locks
                .acquire(requests, None)
                .await
                .map_err(|error| ToolError::new(error.to_string()))?,
        )
    };
    let mut scope =
        crate::bound_tool_batch::admitted_effect_scope(request, descriptor, current, &resolver)?;
    if executor_owns_admission {
        scope.complete = false;
        scope.targets.clear();
    } else {
        for (path, identity) in identities {
            if scope
                .targets
                .iter()
                .filter(|target| target.declared_scope.target.as_ref() == Some(&path))
                .any(|target| target.path != identity)
            {
                return Err(ToolError::new(
                    "root path identity changed while acquiring canonical locks",
                ));
            }
        }
        validate_root_physical_scope(&resolver, &scope)?;
    }
    Ok(LockedRootEffectScope {
        scope,
        resolver,
        _lease: lease,
    })
}

fn model_tool_output_feedback(
    tool_name: &str,
    output: harness_contract::context::ToolOutputDraft,
) -> (
    harness_contract::context::ToolOutputDraft,
    bool,
    Option<ToolFailureKind>,
) {
    let is_error =
        crate::tool_host::bounded_process_feedback_is_error(tool_name, output.observations());
    let failure = is_error.then_some(ToolFailureKind::ExecutionError);
    (output, is_error, failure)
}

fn replay_tool_output(
    outcome: crate::RuntimeToolExecutionOutcome,
) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
    if outcome.status == crate::RuntimeToolExecutionStatus::Executed {
        if let Some(artifact_ref) =
            outcome
                .observed_evidence
                .iter()
                .find_map(|observed| match &observed.target {
                    harness_contract::context::EvidenceTargetIdentity::ToolExecution {
                        execution,
                    } => Some(execution.body.clone()),
                    _ => None,
                })
        {
            Ok(
                harness_contract::context::ToolOutputDraft::ObservedArtifact {
                    summary: outcome.output.unwrap_or_default(),
                    artifact_ref,
                    observations: outcome.observed_evidence,
                },
            )
        } else {
            Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
                outcome.output.unwrap_or_default(),
            ))
        }
    } else {
        Err(ToolError::new(
            outcome
                .error
                .or(outcome.output)
                .unwrap_or_else(|| "durable tool effect failed".into()),
        ))
    }
}

struct TicketToolContext<'context, 'runtime, C, T> {
    inner: &'context ConversationGovernedToolContext<'runtime, C, T>,
    owner: Option<&'context (harness_contract::projection::RuntimeActivityBinding, u32)>,
    preflight_refusals: &'context Mutex<Vec<(String, String)>>,
}

impl<C: ApiClient + Sync, T: ToolExecutor> GovernedToolExecutionContext
    for TicketToolContext<'_, '_, C, T>
{
    type Output = (ConversationMessage, Option<String>);
    type Admission = Option<crate::ToolExecutionAdmission>;
    type Receipt = (ConversationMessage, Option<String>);

    fn local_ceiling(&self) -> usize {
        self.inner.local_ceiling()
    }
    fn precompleted(
        &self,
        task: &crate::GovernedToolPlanTask,
    ) -> Option<(GovernedToolTaskTerminal<Self::Output>, Self::Receipt)> {
        self.inner.precompleted(task)
    }
    fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
    fn wait_for_cancellation(&self) -> GovernedToolFuture<'_, ()> {
        self.inner.wait_for_cancellation()
    }
    fn wait_for_admission_change(&self) -> GovernedToolFuture<'_, ()> {
        self.inner.wait_for_admission_change()
    }
    fn cancel_active(&self, task_id: &str) {
        self.inner.cancel_active(task_id);
    }
    fn try_admit<'future>(
        &'future self,
        task: &'future crate::GovernedToolPlanTask,
    ) -> GovernedToolFuture<'future, GovernedToolAdmission<Self::Admission>> {
        self.inner.try_admit(task)
    }
    fn execute<'future>(
        &'future self,
        task: &'future crate::GovernedToolPlanTask,
        admission: &'future mut Self::Admission,
    ) -> GovernedToolFuture<'future, Result<Self::Output, String>> {
        boxed_tool_future(|| async move {
            let input = self
                .inner
                .pending_tool_uses
                .get(task.original_call_index)
                .ok_or_else(|| "tool ticket references missing original call".to_string())?
                .2
                .as_str();
            self.inner
                .runtime
                .execute_single_tool_at(
                    task,
                    self.inner.plan_id,
                    self.inner.plan_revision,
                    input,
                    self.inner.prompter,
                    self.inner.iterations,
                    admission,
                    self.owner,
                    Some(self.preflight_refusals),
                )
                .await
                .map(|message| self.inner.runtime.collect_tool_result_message(message).1)
                .map_err(|error| error.to_string())
        })
    }
    fn classify_output(&self, output: &Self::Output) -> Result<(), String> {
        self.inner.classify_output(output)
    }
    fn commit_terminal<'future>(
        &'future self,
        task: &'future crate::GovernedToolPlanTask,
        terminal: &'future GovernedToolTaskTerminal<Self::Output>,
    ) -> GovernedToolFuture<'future, Result<Self::Receipt, String>> {
        if let Some(reason) = terminal.undispatched_reason() {
            self.preflight_refusals
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((task.tool_call_id.clone(), reason));
        }
        self.inner.commit_terminal(task, terminal)
    }
    fn on_task_started(&self, task: &crate::GovernedToolPlanTask) {
        self.inner.on_task_started(task);
    }
    fn on_task_terminal(
        &self,
        task: &crate::GovernedToolPlanTask,
        terminal: &GovernedToolTaskTerminal<Self::Output>,
        receipt: Option<&Self::Receipt>,
    ) {
        self.inner.on_task_terminal(task, terminal, receipt);
    }
}

impl<C, T> ConversationRuntime<C, T>
where
    C: ApiClient,
    T: ToolExecutor,
{
    fn root_effect_context(&self) -> Option<crate::CowdExecutionContext> {
        let bus = self.cowd_bus.as_ref()?;
        if bus
            .current_activity_binding()
            .is_some_and(|binding| binding.agent_run_id.is_some())
        {
            return None;
        }
        bus.current_execution_context()
    }

    pub(crate) fn scoped_root_node_activity(
        &mut self,
        ticket: &crate::execution_core::NodeExecutionTicket,
        root: bool,
    ) -> Result<RootNodeActivityRuntime<'_, C, T>, RuntimeError> {
        let original_bus = self.cowd_bus.clone();
        if root {
            let (owner, _) = self.root_effect_owner_for_ticket(ticket)?;
            let view = self
                .cowd_bus()
                .ok_or_else(|| RuntimeError::new("root node has no bus"))?
                .root_node_activity_view(owner.clone())
                .map_err(RuntimeError::new)?;
            if let Some(identity) = self.execution_identity.as_ref().filter(|identity| {
                identity.kind() == harness_contract::execution::ExecutionIdentityKind::TaskGraph
            }) {
                if identity.session_id() != Some(owner.session_id.as_str())
                    || identity.turn_id() != Some(owner.turn_id.as_str())
                    || identity.task_id() != Some(owner.task_id.as_str())
                {
                    return Err(RuntimeError::new(
                        "root checkpoint identity has foreign canonical lineage",
                    ));
                }
                self.execution_identity = Some(
                    harness_contract::execution::ExecutionIdentity::for_task_graph(
                        identity.principal_id(),
                        identity.workspace_id(),
                        identity
                            .mission_id()
                            .ok_or_else(|| RuntimeError::new("root checkpoint has no mission"))?,
                        &owner.task_id,
                        &owner.session_id,
                        &owner.turn_id,
                        &ticket.graph_id,
                    )
                    .map_err(|error| RuntimeError::new(error.to_string()))?,
                );
            }
            self.cowd_bus = Some(Arc::new(view));
        }
        Ok(RootNodeActivityRuntime {
            runtime: self,
            original_bus,
        })
    }

    pub(crate) fn root_effect_owner(
        &self,
    ) -> Result<(harness_contract::projection::RuntimeActivityBinding, u32), RuntimeError> {
        let root = self.root_effect_context().ok_or_else(|| {
            RuntimeError::new("root effect requires its original execution context")
        })?;
        let owner = self
            .cowd_bus
            .as_ref()
            .and_then(|bus| bus.current_activity_binding())
            .ok_or_else(|| {
                RuntimeError::new("root effect requires its original activity binding")
            })?;
        let store = self
            .runtime_event_store
            .as_ref()
            .ok_or_else(|| RuntimeError::new("root effect requires its durable event store"))?;
        let graph = crate::execution_core::graph::ExecutionGraphStateStore::new(Arc::clone(store))
            .load(&root.execution_id)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let (owner, attempt) = authenticated_root_effect_owner(&root, owner, &graph)?;
        let node_id = owner.node_id.as_deref().unwrap();
        let original = store
            .latest_for_stream(&format!("{}:node:{node_id}", graph.id))
            .map_err(|error| RuntimeError::new(error.to_string()))?
            .ok_or_else(|| RuntimeError::new("root effect has no durable node activity"))?;
        if original.activity_binding().as_ref() != Some(&owner)
            || original.payload["to"] != "running"
            || original.kind != "execution_node.transitioned"
            || original.actor.as_deref() != Some("execution_commit_service")
            || original.scope != RuntimeEventScope::ExecutionNode
            || original.status.as_deref() != Some("running")
        {
            return Err(RuntimeError::new(
                "root effect original activity revision/fence/generation mismatch",
            ));
        }
        Ok((owner, attempt))
    }

    pub(crate) fn root_effect_owner_for_ticket(
        &self,
        ticket: &crate::execution_core::NodeExecutionTicket,
    ) -> Result<(harness_contract::projection::RuntimeActivityBinding, u32), RuntimeError> {
        let root = self
            .root_effect_context()
            .ok_or_else(|| RuntimeError::new("root ticket has no execution context"))?;
        let live = self
            .cowd_bus
            .as_ref()
            .and_then(|bus| bus.current_activity_binding())
            .ok_or_else(|| RuntimeError::new("root ticket has no activity source"))?;
        live.validate().map_err(RuntimeError::new)?;
        let store = self
            .runtime_event_store
            .as_ref()
            .ok_or_else(|| RuntimeError::new("root ticket has no durable source"))?;
        let graph = crate::execution_core::graph::ExecutionGraphStateStore::new(Arc::clone(store))
            .load(&ticket.graph_id)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let original = store
            .latest_for_stream(&format!("{}:node:{}", ticket.graph_id, ticket.node_id))
            .map_err(|error| RuntimeError::new(error.to_string()))?
            .ok_or_else(|| RuntimeError::new("root ticket has no original node activity"))?;
        let owner = original
            .activity_binding()
            .ok_or_else(|| RuntimeError::new("root ticket node has no activity binding"))?;
        let (owner, attempt) = authenticated_root_effect_owner(&root, owner, &graph)?;
        let start_revision = original.payload["graph_revision"]
            .as_u64()
            .filter(|revision| *revision > 0 && *revision <= graph.revision)
            .ok_or_else(|| {
                RuntimeError::new("root ticket original node has no graph start revision")
            })?;
        let start_events = store
            .list_stream_after(
                &ticket.graph_id,
                start_revision - 1,
                start_revision,
                1,
                4 * 1024 * 1024,
            )
            .map_err(RuntimeError::new)?;
        let start = start_events
            .first()
            .filter(|event| {
                event.sequence == start_revision
                    && event.transaction_id == original.transaction_id
                    && event.commit_cursor == original.commit_cursor
            })
            .ok_or_else(|| RuntimeError::new("root ticket has no atomic original graph start"))?;
        let start_event = original_ticket_start_event(store, start)?;
        let start_binding = match &start_event {
            crate::execution_core::graph::ExecutionGraphEvent::NodeTransitioned {
                node_id,
                binding,
                ..
            } if node_id == &ticket.node_id => binding.as_ref(),
            crate::execution_core::graph::ExecutionGraphEvent::NodesStarted {
                bindings, ..
            }
            | crate::execution_core::graph::ExecutionGraphEvent::Checkpoint { bindings, .. } => {
                bindings.get(&ticket.node_id)
            }
            _ => None,
        }
        .ok_or_else(|| RuntimeError::new("root ticket has no original execution start binding"))?;
        if attempt != ticket.attempt
            || owner.node_id.as_deref() != Some(ticket.node_id.as_str())
            || start_binding.attempt != ticket.attempt
            || start_binding.executor_kind != ticket.executor_kind
            || start_binding.ticket_idempotency_key != ticket.idempotency_key
            || ticket.service_class != graph.service_class
            || live.root_execution_id != ticket.graph_id
            || live.session_id != owner.session_id
            || live.turn_id != owner.turn_id
            || live.root_task_id != owner.root_task_id
            || live.task_id != owner.task_id
            || live.generation != owner.generation
            || original.payload["to"] != "running"
            || original.kind != "execution_node.transitioned"
            || original.actor.as_deref() != Some("execution_commit_service")
            || original.scope != RuntimeEventScope::ExecutionNode
            || original.status.as_deref() != Some("running")
            || live.tool_call_id.is_some()
            || live.tool_contract_id.is_some()
            || live
                .node_id
                .as_ref()
                .is_some_and(|node_id| node_id != &ticket.node_id)
            || (live.node_id.is_some() && live != owner)
            || !graph.nodes.iter().any(|node| {
                node.id == ticket.node_id
                    && node.executor_kind == ticket.executor_kind
                    && node.payload_ref == ticket.payload_ref
            })
        {
            return Err(RuntimeError::new(
                "root ticket graph/node/attempt disagrees with its activity source",
            ));
        }
        Ok((owner, attempt))
    }

    /// Execute one graph-owned tool wave. All tool side effects in a normal
    /// conversation turn enter through this method.
    pub(crate) async fn execute_tool_batch_step(
        &self,
        calls: &[ModelToolCall],
        prompter: &crate::permissions::SharedPrompter,
        iteration: usize,
    ) -> Result<ToolBatchStepResult, RuntimeError>
    where
        C: Sync,
    {
        self.execute_tool_batch_step_with_replays(
            calls,
            prompter,
            iteration,
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
    }

    pub(crate) async fn execute_tool_batch_step_with_replays(
        &self,
        calls: &[ModelToolCall],
        prompter: &crate::permissions::SharedPrompter,
        iteration: usize,
        replays: &BTreeMap<String, EarlyToolExecutionReceipt>,
        replay_invocations: &BTreeMap<String, harness_contract::tool::GovernedToolInvocation>,
    ) -> Result<ToolBatchStepResult, RuntimeError>
    where
        C: Sync,
    {
        self.execute_tool_batch_step_with_owner(
            calls,
            prompter,
            iteration,
            replays,
            replay_invocations,
            None,
        )
        .await
        .map(|(result, _)| result)
    }

    pub(crate) async fn execute_tool_batch_step_with_owner(
        &self,
        calls: &[ModelToolCall],
        prompter: &crate::permissions::SharedPrompter,
        iteration: usize,
        replays: &BTreeMap<String, EarlyToolExecutionReceipt>,
        replay_invocations: &BTreeMap<String, harness_contract::tool::GovernedToolInvocation>,
        owner: Option<&(harness_contract::projection::RuntimeActivityBinding, u32)>,
    ) -> Result<
        (
            ToolBatchStepResult,
            Vec<harness_contract::tool::GovernedToolInvocation>,
        ),
        RuntimeError,
    >
    where
        C: Sync,
    {
        let execution = self
            .cowd_bus
            .as_ref()
            .and_then(|bus| bus.current_execution_context());
        let admitted_turn = execution
            .as_ref()
            .map(|context| context.turn_id.clone())
            .or_else(|| {
                self.session_input_stream
                    .active_turn_id()
                    .map(|turn| turn.to_string())
            });
        if calls.iter().any(|call| {
            call.binding.as_ref().is_some_and(|binding| {
                binding.snapshot.identity.session_id != self.session_id()
                    || admitted_turn
                        .as_ref()
                        .is_some_and(|turn| binding.snapshot.identity.turn_id != *turn)
            })
        }) {
            return Err(RuntimeError::new("tool_step_session_or_turn_mismatch"));
        }
        if self.cancellation_token.is_cancelled() {
            return Err(RuntimeError::new("turn cancelled before tool execution"));
        }
        self.remember_native_tool_calls(calls)?;
        let mut refused_before_dispatch = Vec::new();
        use crate::tool_dispatch::ToolRequest;

        // The hard repetition breaker lives in `execute_single_tool`, which this
        // path never enters. A delegated Agent's calls arrive here instead, so
        // the same bytes could be dispatched without any bound: one live run
        // committed the identical artifact eleven times against a block bound of
        // six. The counter already covers this path - `record_repeated_calls`
        // runs on the common path after both branches - so the count was being
        // kept and nothing acted on it. Refuse here exactly as the root path
        // does, and record each refusal the same way it already records itself.
        let mut refused_ids = std::collections::BTreeSet::new();
        let mut refusal_messages = Vec::new();
        for call in calls {
            if replays.contains_key(&call.id) {
                continue;
            }
            let Some(refusal) =
                self.blocked_repeated_call(&call.name, &call.id, &call.input, iteration)
            else {
                continue;
            };
            self.note_refusal_ignored(&call.name, &call.input);
            refused_before_dispatch.push((
                call.id.clone(),
                "repetition breaker refused before compilation".into(),
            ));
            refused_ids.insert(call.id.clone());
            refusal_messages.push(refusal);
        }
        for refusal in &refusal_messages {
            self.session
                .write()
                .await
                .push_message(refusal.clone())
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            let sequence = self.session_head().await.message_count.wrapping_sub(1);
            self.record_message_event(refusal, sequence);
            self.remember_tool_trace_from_message(refusal);
        }
        if refused_ids.len() == calls.len() {
            // Every call in this batch was refused, so there is nothing to
            // compile. Answer with the refusals rather than an empty plan the
            // model cannot act on.
            return Ok((
                ToolBatchStepResult {
                    refused_before_dispatch,
                    failed: refusal_messages.len(),
                    messages: refusal_messages,
                    max_concurrency_observed: 0,
                    parallel_batches: 0,
                },
                Vec::new(),
            ));
        }
        let mut requests = calls
            .iter()
            .filter(|call| !refused_ids.contains(&call.id))
            .map(|call| ToolRequest {
                tool_use_id: call.id.clone(),
                tool_name: call.name.clone(),
                input: call.input.clone(),
                depends_on: call.depends_on.clone(),
            })
            .collect::<Vec<_>>();
        let _ = crate::intent_planner::infer_tool_dependencies(&mut requests);
        let pending = calls
            .iter()
            .filter(|call| !refused_ids.contains(&call.id))
            .map(|call| (call.id.clone(), call.name.clone(), call.input.clone()))
            .collect::<Vec<_>>();
        let mut prepared = Vec::with_capacity(requests.len());
        let mut admission_rejections = BTreeMap::new();
        for request in &requests {
            if let Some(invocation) = replay_invocations.get(&request.tool_use_id) {
                prepared.push(invocation.clone());
            } else {
                match self
                    .tool_executor
                    .prepare_step_invocations(std::slice::from_ref(request), calls)
                {
                    Ok(invocations) => prepared.extend(invocations),
                    Err(error) => {
                        admission_rejections.insert(
                            request.tool_use_id.clone(),
                            format!("step admission refused: {error}"),
                        );
                    }
                }
            }
        }
        let workspace_root = self.governed_workspace_root()?;
        let compilation = GovernedToolCompiler.compile_prepared_partial(
            &workspace_root,
            &requests,
            &prepared,
            &admission_rejections,
        );
        let mut compilation = match compilation {
            Ok(compilation) => compilation,
            Err(error) => {
                let reason = format!("governed tool DAG rejected before execution: {error}");
                self.append_execution_runtime_event(
                    RuntimeEventScope::Tool,
                    "tool.plan.rejected",
                    Some("rejected".to_string()),
                    calls
                        .iter()
                        .map(|call| RuntimeEventRef {
                            kind: "tool_call".to_string(),
                            id: call.id.clone(),
                        })
                        .collect(),
                    serde_json::json!({
                        "reason": error.to_string(),
                        "tool_count": calls.len(),
                    }),
                );
                let mut messages = Vec::with_capacity(calls.len());
                for call in calls {
                    if !replays.contains_key(&call.id) {
                        refused_before_dispatch.push((call.id.clone(), reason.clone()));
                    }
                    let message = ConversationMessage::tool_result(
                        call.id.clone(),
                        call.name.clone(),
                        reason.clone(),
                        true,
                    );
                    self.session
                        .write()
                        .await
                        .push_message(message.clone())
                        .map_err(|error| RuntimeError::new(error.to_string()))?;
                    let sequence = self.session_head().await.message_count.wrapping_sub(1);
                    self.record_message_event(&message, sequence);
                    self.remember_tool_trace_from_message(&message);
                    messages.push(message);
                }
                return Ok((
                    ToolBatchStepResult {
                        refused_before_dispatch,
                        failed: messages.len(),
                        messages,
                        max_concurrency_observed: 0,
                        parallel_batches: 0,
                    },
                    prepared,
                ));
            }
        };
        if let Some(plan) = compilation.plan.as_mut() {
            crate::step_snapshot::bind_plan_calls(plan, calls);
        }
        let mut preflight_messages = Vec::with_capacity(compilation.rejected.len());
        for rejected in &compilation.rejected {
            if !replays.contains_key(&rejected.tool_call_id) {
                refused_before_dispatch.push((
                    rejected.tool_call_id.clone(),
                    format!(
                        "governed tool node rejected before execution: {}",
                        rejected.reason
                    ),
                ));
            }
            let message = ConversationMessage::tool_result(
                rejected.tool_call_id.clone(),
                rejected.tool_name.clone(),
                format!(
                    "governed tool node rejected before execution: {}",
                    rejected.reason
                ),
                true,
            );
            self.session
                .write()
                .await
                .push_message(message.clone())
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            let sequence = self.session_head().await.message_count.wrapping_sub(1);
            self.record_message_event(&message, sequence);
            self.remember_tool_trace_from_message(&message);
            preflight_messages.push(message);
        }
        if !compilation.rejected.is_empty() {
            self.append_execution_runtime_event(
                RuntimeEventScope::Tool,
                "tool.plan.partially_rejected",
                Some("partial".to_string()),
                compilation
                    .rejected
                    .iter()
                    .map(|rejected| RuntimeEventRef {
                        kind: "tool_call".to_string(),
                        id: rejected.tool_call_id.clone(),
                    })
                    .collect(),
                serde_json::json!({
                    "rejected": compilation.rejected,
                    "accepted_count": compilation.plan.as_ref().map_or(0, |plan| plan.task_count),
                }),
            );
        }
        let Some(plan) = compilation.plan else {
            // The caller rolls back eagerly written tool messages and republishes
            // from this result, so a refusal that is not in here is a refusal the
            // model never sees.
            let mut messages = refusal_messages;
            messages.extend(preflight_messages);
            let failed = count_failed_tool_results(&messages);
            return Ok((
                ToolBatchStepResult {
                    refused_before_dispatch,
                    failed,
                    messages,
                    max_concurrency_observed: 0,
                    parallel_batches: 0,
                },
                prepared,
            ));
        };
        self.record_governed_tool_plan(&plan)?;
        // Accountability: a call the model emitted that is neither scheduled
        // nor refused was swallowed, and a swallowed call is invisible to the
        // model, the ledger and the operator. Report it and tell the model,
        // rather than letting it disappear the way mon6's objective_review did.
        let swallowed = unaccounted_calls(&pending, &plan, &compilation.rejected);
        for (call_id, tool_name) in &swallowed {
            if !replays.contains_key(call_id) {
                refused_before_dispatch.push((
                    call_id.clone(),
                    "compiler did not schedule this call".into(),
                ));
            }
            let input_preview = pending
                .iter()
                .find(|(id, _, _)| id == call_id)
                .map(|(_, _, input)| input.chars().take(200).collect::<String>())
                .unwrap_or_default();
            self.append_execution_runtime_event(
                RuntimeEventScope::Tool,
                "tool.call.unaccounted",
                Some("unaccounted".to_string()),
                vec![RuntimeEventRef {
                    kind: "tool_call".to_string(),
                    id: call_id.clone(),
                }],
                serde_json::json!({
                    "tool_name": tool_name,
                    "input": input_preview,
                    "reason": "neither scheduled by the governed plan nor refused; dropped without a record",
                }),
            );
            tracing::warn!(
                target: "cowd::tool_accountability",
                tool = %tool_name,
                tool_call_id = %call_id,
                "a model call was dropped without being scheduled or refused"
            );
            let message = ConversationMessage::tool_result(
                call_id.clone(),
                tool_name.clone(),
                "Runtime dropped this call without scheduling or refusing it: it was never \
                 executed. Issue it again, or take a different action."
                    .to_string(),
                true,
            );
            self.session
                .write()
                .await
                .push_message(message.clone())
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            let sequence = self.session_head().await.message_count.wrapping_sub(1);
            self.record_message_event(&message, sequence);
            self.remember_tool_trace_from_message(&message);
            preflight_messages.push(message);
        }
        let mut admission_plan = plan.clone();
        admission_plan
            .tasks
            .retain(|task| !replays.contains_key(&task.tool_call_id));
        let decision = self
            .active_turn_strategy()
            .map(|state| state.decision)
            .ok_or_else(|| RuntimeError::new("tool batch has no admitted turn identity"))?;
        self.tool_executor.bind_execution_decision(decision.clone());
        let mut validation = admission_plan.validate_against_execution_decision(&decision);
        if validation.allowed {
            self.satisfy_tool_strategy_gates(&decision, &admission_plan, &mut validation, prompter)
                .await;
        }
        self.record_tool_strategy_validation(&validation, self.session_head().await.message_count);
        let mut max_concurrency_observed = 0;
        let mut parallel_batches = 0;
        let mut messages = refusal_messages;
        messages.extend(preflight_messages);
        let preflight_refusals = Mutex::new(Vec::new());
        if validation.allowed {
            self.record_tool_schedule(&plan, &requests, self.session_head().await.message_count);
            let mut precompleted = BTreeMap::new();
            for call in calls {
                if let Some(receipt) = replays.get(&call.id) {
                    let output = receipt
                        .outcome
                        .output
                        .as_deref()
                        .or(receipt.outcome.error.as_deref())
                        .unwrap_or_default();
                    let message = self
                        .prepare_governed_tool_result_with_invocation(
                            &call.id,
                            &call.name,
                            &call.input,
                            output,
                            receipt.outcome.model_feedback_is_error(),
                            None,
                        )
                        .await?;
                    precompleted
                        .insert(call.id.clone(), self.collect_tool_result_message(message).1);
                }
            }
            let context = ConversationGovernedToolContext {
                runtime: self,
                pending_tool_uses: &pending,
                prompter,
                iterations: iteration,
                plan_id: &plan.plan_id,
                plan_revision: plan.revision,
                precompleted: &precompleted,
            };
            let ticket_context = TicketToolContext {
                inner: &context,
                owner,
                preflight_refusals: &preflight_refusals,
            };
            let report = GovernedToolExecutor.execute(&plan, &ticket_context).await;
            max_concurrency_observed = report.max_active;
            parallel_batches = usize::from(report.max_active > 1);
            for outcome in report.outcomes {
                let Some((message, _)) = outcome.receipt else {
                    return Err(RuntimeError::new(format!(
                        "governed tool task `{}` reached terminal state without a durable result receipt",
                        outcome.task_id
                    )));
                };
                self.remember_tool_trace_from_message(&message);
                messages.push(message);
            }
        } else {
            let reason = format!(
                "runtime strategy lease `{}` denied tool batch: {}",
                validation.lease_id,
                validation.findings.join(", ")
            );
            for call in calls {
                if !replays.contains_key(&call.id) {
                    refused_before_dispatch.push((call.id.clone(), reason.clone()));
                }
                let message = ConversationMessage::tool_result(
                    call.id.clone(),
                    call.name.clone(),
                    reason.clone(),
                    true,
                );
                self.session
                    .write()
                    .await
                    .push_message(message.clone())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                let sequence = self.session_head().await.message_count.wrapping_sub(1);
                self.record_message_event(&message, sequence);
                self.remember_tool_trace_from_message(&message);
                messages.push(message);
            }
        }
        refused_before_dispatch.extend(
            preflight_refusals
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let failed = count_failed_tool_results(&messages);
        Ok((
            ToolBatchStepResult {
                refused_before_dispatch,
                messages,
                failed,
                max_concurrency_observed,
                parallel_batches,
            },
            prepared,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn finalize_graph_turn(
        &mut self,
        user_input: &str,
        final_answer: String,
        assistant_messages: Vec<ConversationMessage>,
        tool_results: Vec<ConversationMessage>,
        iterations: usize,
        model: Option<String>,
        models_used: Vec<String>,
        first_token_latency_ms: Option<u64>,
        active_stream_duration_ms: u64,
        input_tokens: u64,
        output_tokens: u64,
        wall_duration_ms: u64,
        duplicate_tool_calls: u64,
        write_attempt_paths: Vec<String>,
        max_tool_concurrency_observed: usize,
        parallel_tool_batches: usize,
        terminal_completion: harness_contract::goal::GoalCompletion,
        defer_post_turn_memory_maintenance: bool,
        recovered_telemetry: Option<crate::RunModelTelemetry>,
    ) -> Result<TurnSummary, RuntimeError> {
        let finalize_started = Instant::now();
        let replaying_terminal = recovered_telemetry.is_some();
        let recovered_provider_ms = recovered_telemetry
            .as_ref()
            .filter(|telemetry| {
                telemetry.usage_source.starts_with("provider") && telemetry.wall_duration_ms > 0
            })
            .map(|telemetry| telemetry.wall_duration_ms);
        if final_answer.trim().is_empty() {
            return Err(RuntimeError::new("model produced an empty final answer"));
        }
        if self.active_turn_strategy().is_none() {
            return Err(RuntimeError::new(
                "turn finalization requires the Host-admitted turn strategy owner",
            ));
        }
        let decision = self
            .active_turn_strategy()
            .map(|state| state.decision)
            .ok_or_else(|| RuntimeError::new("turn finalization has no strategy owner"))?;
        let mut kernel = RuntimeAiKernel::begin_turn_with_execution_decision(
            self.session_id().to_string(),
            user_input.to_string(),
            self.context_profile(),
            &self.system_prompt,
            decision,
        );
        if let Ok(plans) = self.turn_governed_tool_plans.lock() {
            for plan in plans.iter().cloned() {
                kernel.record_governed_tool_plan(plan);
            }
        }
        if !matches!(
            terminal_completion,
            harness_contract::goal::GoalCompletion::Satisfied
        ) {
            kernel.record_terminal_blocked(
                "the execution graph reached a non-satisfied terminal completion",
            );
        }
        let failed_tools = count_failed_tool_results(&tool_results);
        let ai_kernel_trace = kernel.finalize(
            &final_answer,
            tool_results.len().saturating_sub(failed_tools),
            failed_tools,
        );
        // Request-preflight owns compaction. Finalization never rewrites a
        // healthy transcript merely because an aggregate token estimate grew.
        let auto_compaction = self
            .turn_preflight_compaction
            .lock()
            .ok()
            .and_then(|mut receipt| receipt.take());
        let compaction_elapsed = Duration::ZERO;
        let memory_started = Instant::now();
        if recovered_telemetry.is_none() {
            if defer_post_turn_memory_maintenance {
                self.schedule_memory_post_turn(user_input).await;
            } else {
                let _ = self.run_memory_post_turn(user_input).await;
            }
            self.memory_context_revision.fetch_add(1, Ordering::AcqRel);
        }
        let memory_elapsed = memory_started.elapsed();
        let usage = if let Some(telemetry) = recovered_telemetry.as_ref() {
            TokenUsage {
                input_tokens: u32::try_from(telemetry.input_tokens)
                    .map_err(|_| RuntimeError::new("recovered input usage overflow"))?,
                output_tokens: u32::try_from(telemetry.output_tokens)
                    .map_err(|_| RuntimeError::new("recovered output usage overflow"))?,
                cache_creation_input_tokens: u32::try_from(telemetry.cache_create_tokens)
                    .map_err(|_| RuntimeError::new("recovered cache-create usage overflow"))?,
                cache_read_input_tokens: u32::try_from(telemetry.cache_read_tokens)
                    .map_err(|_| RuntimeError::new("recovered cache-read usage overflow"))?,
            }
        } else {
            self.usage_tracker.cumulative_usage()
        };
        let telemetry =
            recovered_telemetry.unwrap_or_else(|| crate::cowd_event::RunModelTelemetry {
                model: model.clone(),
                models_used,
                first_token_latency_ms,
                active_stream_duration_ms: Some(active_stream_duration_ms.max(1)),
                wall_duration_ms: wall_duration_ms.max(1),
                output_chars: final_answer.chars().count() as u64,
                output_chunks: iterations as u64,
                input_tokens,
                output_tokens,
                cache_create_tokens: u64::from(usage.cache_creation_input_tokens),
                cache_read_tokens: u64::from(usage.cache_read_input_tokens),
                cache_dimensions_known: self.turn_cache_dimensions_known(),
                total_tokens: input_tokens
                    .saturating_add(output_tokens)
                    .saturating_add(u64::from(usage.cache_creation_input_tokens))
                    .saturating_add(u64::from(usage.cache_read_input_tokens)),
                usage_source: "provider".to_string(),
                wall_chars_per_second: rate_per_second(
                    final_answer.chars().count() as u64,
                    wall_duration_ms.max(1),
                ),
                wall_tokens_per_second: rate_per_second(output_tokens, wall_duration_ms.max(1)),
                active_chars_per_second: None,
                active_tokens_per_second: None,
                chars_per_second: rate_per_second(
                    final_answer.chars().count() as u64,
                    wall_duration_ms.max(1),
                ),
                tokens_per_second: rate_per_second(output_tokens, wall_duration_ms.max(1)),
            });
        let context_turn_report = self.build_context_turn_report(
            &ai_kernel_trace.harness_receipt.id,
            usage,
            auto_compaction.clone(),
        );
        self.remember_context_turn_report(context_turn_report.clone())
            .await?;
        let mut assistant_messages = assistant_messages;
        if !matches!(
            terminal_completion,
            harness_contract::goal::GoalCompletion::Satisfied
        ) && !assistant_messages.last().is_some_and(|message| {
            message.role.role_str() == "assistant"
                && matches!(message.blocks.as_slice(), [ContentBlock::Text { text }] if text == &final_answer)
        }) {
            assistant_messages.push(ConversationMessage::assistant(vec![ContentBlock::Text {
                text: final_answer.clone(),
            }]));
        }
        let summary = TurnSummary {
            final_answer,
            terminal_completion,
            assistant_messages,
            tool_results,
            iterations,
            usage,
            model_telemetry: telemetry,
            auto_compaction,
            ai_kernel_trace,
            context_turn_report,
            model_observations: self.turn_model_observations(),
            duplicate_tool_calls,
            write_attempt_paths,
            max_tool_concurrency_observed,
            parallel_tool_batches,
        };
        self.record_turn_completed(&summary);
        self.record_ai_kernel_trace_event(
            &summary.ai_kernel_trace,
            self.session_head().await.message_count,
        );
        if let Some(ref cowd) = self.cowd_bus {
            cowd.emit(crate::cowd_event::CowdEvent::WriteAttemptsObserved {
                paths: summary.write_attempt_paths.clone(),
            });
            cowd.emit(crate::cowd_event::CowdEvent::RunModelTelemetry {
                telemetry: summary.model_telemetry.clone(),
            });
        }
        let commit_ms = finalize_started
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        crate::execution_core::performance::record_turn_latency_trace(
            crate::execution_core::performance::TurnLatencyTrace {
                trace_id: summary.ai_kernel_trace.harness_receipt.id.clone(),
                session_id: self.session_id().to_string(),
                turn_id: None,
                activation_ms: None,
                context_ms: None,
                provider_ms: if replaying_terminal {
                    recovered_provider_ms
                } else {
                    Some(wall_duration_ms)
                },
                tool_ms: None,
                commit_ms: Some(commit_ms),
                total_ms: if replaying_terminal {
                    recovered_provider_ms
                        .unwrap_or_default()
                        .saturating_add(commit_ms)
                } else {
                    wall_duration_ms.saturating_add(commit_ms)
                },
                recorded_at_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |duration| duration.as_millis() as u64),
            },
        );
        tracing::debug!(
            total_ms = finalize_started.elapsed().as_millis(),
            compaction_ms = compaction_elapsed.as_millis(),
            memory_post_turn_ms = memory_elapsed.as_millis(),
            post_turn_memory_deferred = defer_post_turn_memory_maintenance,
            "graph turn finalization completed"
        );
        Ok(summary)
    }

    pub(super) async fn satisfy_tool_strategy_gates(
        &self,
        execution_decision: &crate::execution_core::RuntimeExecutionDecision,
        admission_plan: &crate::GovernedToolPlan,
        validation: &mut GovernedToolPolicyValidationReport,
        prompter: &crate::permissions::SharedPrompter,
    ) {
        // `requires_approval` is a scheduling signal only. Authorization is
        // deliberately resolved per concrete Tool effect below; a batch-level
        // boolean can never authorize sibling calls with different hashes.

        if !validation.requires_checkpoint {
            return;
        }
        if !self.tool_executor.has_tool("checkpoint") {
            validation.allowed = false;
            validation
                .findings
                .push("checkpoint_create_tool_unavailable".to_string());
            return;
        }

        let paths = match checkpoint_paths_for_admission(
            admission_plan,
            self.tool_executor.pin_step_catalog(),
        ) {
            Ok(paths) => paths,
            Err(error) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_scope_unproven:{error}"));
                return;
            }
        };
        let checkpoint_input = serde_json::json!({
            "action": "create",
            "label": format!(
                "runtime strategy lease {} before high-risk mutation",
                execution_decision.lease.lease_id
            ),
            "paths": paths
        })
        .to_string();
        let executor = Arc::clone(&self.tool_executor);
        let checkpoint_value = match serde_json::from_str::<serde_json::Value>(&checkpoint_input) {
            Ok(value) => value,
            Err(error) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_input_invalid:{error}"));
                return;
            }
        };
        let binding = match self.bind_system_tool_call(
            "checkpoint",
            &checkpoint_value,
            &format!("strategy:{}", execution_decision.lease.lease_id),
        ) {
            Ok(binding) => binding,
            Err(error) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_origin_rejected:{error}"));
                return;
            }
        };
        let Some(descriptor) = executor.registered_tool_effect("checkpoint", &checkpoint_value)
        else {
            validation.allowed = false;
            validation
                .findings
                .push("checkpoint_create_missing_effect_descriptor".to_string());
            return;
        };
        let timeout = self.tool_timeout.unwrap_or_else(|| {
            Duration::from_secs(
                crate::ToolSafetyCategory::from_effect(&descriptor).default_timeout_secs(),
            )
        });
        let authorization = match self
            .negotiate_tool_authorization(
                &descriptor,
                &checkpoint_input,
                format!(
                    "{}:checkpoint:{}",
                    self.session_id().to_string(),
                    execution_decision.lease.lease_id
                ),
                PermissionContext::default(),
                timeout.as_secs(),
                prompter,
            )
            .await
        {
            Ok(ToolAuthorizationDecision::Authorized(decision)) => decision,
            Ok(ToolAuthorizationDecision::Gap { assessment, .. }) => {
                validation.allowed = false;
                validation.findings.push(format!(
                    "checkpoint_authorization_denied:{}",
                    assessment
                        .gap
                        .as_ref()
                        .map_or("unknown capability gap", |gap| gap.reason.as_str())
                ));
                return;
            }
            Err(error) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_authorization_denied:{error}"));
                return;
            }
        };
        let checkpoint_demand = crate::governed_tool_plan::resource_demand_from_effect(&descriptor);
        let result = self
            .tool_execution_plane
            .execute_async_classified_retained(
                &checkpoint_demand,
                Some(timeout),
                self.execution_service_class,
                Some(self.execution_service_class),
                Some(self.session_id()),
                async move {
                    let call_id = binding
                        .as_ref()
                        .map(|binding| binding.origin.call_id.as_str())
                        .unwrap_or("offline-checkpoint");
                    executor
                        .execute_step_authorized_invocation_output(
                            binding.as_ref(),
                            call_id,
                            &authorization.authorization,
                            "checkpoint",
                            &checkpoint_input,
                        )
                        .await
                },
            )
            .await;
        let result = result.0;
        match result {
            Ok(Ok(output)) => {
                validation.checkpoint_created = true;
                tracing::info!(
                    strategy_lease_id = %execution_decision.lease.lease_id,
                    checkpoint = %preview_chars(output.model_text(), 240),
                    "strategy checkpoint created before mutation"
                );
            }
            Ok(Err(error)) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_creation_failed:{error}"));
            }
            Err(error) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_execution_failed:{error}"));
            }
        }
    }

    /// Extract the per-tool execution logic from run_turn for reuse.
    async fn execute_scoped_root_effect<Operation, Execution>(
        &self,
        commit: &crate::execution_core::graph::ExecutionCommitService,
        request: &crate::RuntimeToolExecutionRequest,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        root: Option<&crate::CowdExecutionContext>,
        owner: Option<&(harness_contract::projection::RuntimeActivityBinding, u32)>,
        executor_owns_admission: bool,
        execute: Operation,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError>
    where
        C: Sync,
        Operation: FnOnce() -> Execution + Send,
        Execution: std::future::Future<
                Output = Result<harness_contract::context::ToolOutputDraft, ToolError>,
            > + Send,
    {
        if let Some(outcome) = commit
            .completed_tool_effect(request, descriptor)
            .map_err(|error| ToolError::new(error.to_string()))?
        {
            return replay_tool_output(crate::bound_tool_batch::retained_replay_outcome(outcome));
        }
        if let Some(root) = root {
            commit.preflight_fresh_root_tool(request, descriptor, root)?;
        }
        let retain_refusal = |error: ToolError| {
            // A scope/resource wait can race closure of the original owner.
            // The original NoDispatch consumer still proves the empty history
            // under its source CAS; this token alone grants no closure.
            if let Some(root) = root {
                if let Err(refusal) = commit.preflight_fresh_root_tool(request, descriptor, root) {
                    if refusal
                        .pre_effect_refusal_for(
                            &request.tool_use_id,
                            &request.tool_name,
                            &request.input,
                            request.step_binding.as_ref(),
                        )
                        .is_some()
                    {
                        return refusal;
                    }
                }
            }
            error
        };
        if request.tool_name == "execute_code" {
            let lease = self.tool_executor.bounded_process_lease(request)?;
            let authority = self.tool_executor.bounded_process_authority(request)?;
            let plane = self
                .tool_executor
                .bounded_process_plane()
                .ok_or_else(|| ToolError::new("bounded process plane unsupported"))?;
            if !Arc::ptr_eq(plane.scope_locks(), self.tool_execution_plane.scope_locks()) {
                return Err(ToolError::new("bounded root canonical manager mismatch"));
            }
            let outcome = crate::bound_tool_batch::execute_bounded_process_owned(
                lease,
                plane,
                authority,
                commit.clone(),
                request.clone(),
                descriptor.clone(),
                root.cloned(),
                owner.cloned(),
            )
            .await
            .map_err(&retain_refusal)?;
            return replay_tool_output(outcome);
        }
        if root.is_none() && descriptor.effect_kind != harness_contract::tool::ToolEffectKind::Read
        {
            commit.preflight_fresh_bound_tool(request, descriptor)?;
        }
        let locked = lock_root_effect_scope(
            request,
            descriptor,
            self.tool_executor.pin_step_catalog(),
            self.tool_execution_plane.scope_locks(),
            executor_owns_admission,
        )
        .await?;
        self.verify_session_execution_fence(crate::SessionExecutionFencePhase::ToolExecution)
            .await
            .map_err(|error| ToolError::new(error.to_string()))?;
        let state = if let Some(root) = root {
            let (owner, attempt) =
                owner.ok_or_else(|| ToolError::new("root effect has no original caller owner"))?;
            if descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Read {
                commit.begin_root_read_acquisition(
                    request,
                    descriptor,
                    root,
                    &locked.scope,
                    owner,
                    *attempt,
                )
            } else {
                commit.begin_root_tool_effect(
                    request,
                    descriptor,
                    root,
                    &locked.scope,
                    owner,
                    *attempt,
                )
            }
        } else {
            commit.begin_bound_tool_effect(request, descriptor, &locked.scope)
        }
        .map_err(|error| retain_refusal(ToolError::new(error.to_string())))?;
        match state {
            crate::execution_core::graph::ToolEffectState::Completed(outcome) => {
                return replay_tool_output(crate::bound_tool_batch::retained_replay_outcome(
                    outcome,
                ))
            }
            crate::execution_core::graph::ToolEffectState::Uncertain => {
                return Err(ToolError::new(
                    "tool effect is uncertain; non-idempotent execution was not replayed",
                ))
            }
            crate::execution_core::graph::ToolEffectState::Fresh
            | crate::execution_core::graph::ToolEffectState::NotRequired => {}
        }
        validate_root_physical_scope(&locked.resolver, &locked.scope)?;
        // A process-class effect is admitted before its command runs: the host
        // executor can only report quiescence after the fact, and the receipt
        // may only be settled against a durable admission.
        let host_process_admission = commit
            .admit_host_process(request, descriptor)
            .await
            .map_err(|error| ToolError::new(error.to_string()))?;
        let execution = execute().await;
        self.verify_session_execution_fence(crate::SessionExecutionFencePhase::ToolCommit)
            .await
            .map_err(|error| ToolError::new(error.to_string()))?;
        let output = execution
            .as_ref()
            .ok()
            .map(|output| output.model_text().to_string());
        let error = execution.as_ref().err().map(ToString::to_string);
        let mut outcome = crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: if execution.is_ok() {
                crate::RuntimeToolExecutionStatus::Executed
            } else {
                crate::RuntimeToolExecutionStatus::Failed
            },
            category: request.category,
            output,
            error,
            evidence_ref: format!("tool-effect:{}", request.idempotency_key),
            observed_evidence: execution
                .as_ref()
                .ok()
                .map(|draft| draft.observations().to_vec())
                .unwrap_or_default(),
        };
        // A process-class effect owns a physical process tree, so its durable
        // receipt must carry the honest quiescence proof. The host shell is
        // not a sandbox process: it can prove that the command is gone and
        // that nothing of it is still alive, and nothing more. The Runtime
        // kernel is the only authority that may publish that fact.
        let host_process_terminal = if execution.is_ok() {
            crate::host_process_closure::mint_host_process_evidence(
                commit,
                request,
                descriptor,
                &mut outcome,
                host_process_admission.as_ref(),
            )
            .await
            .map_err(|error| {
                ToolError::new(format!(
                    "tool `{}` host process terminal failed: {error}",
                    request.tool_name
                ))
            })?
        } else {
            false
        };
        // A read-class effect has no settlement stream for a process terminal,
        // so it keeps its plain read receipt and the witness stays honestly
        // unproven for it.
        let committed = if descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Read {
            if host_process_terminal {
                commit
                    .commit_host_process_read_closure(
                        request,
                        descriptor,
                        &outcome,
                        &locked.scope,
                        root,
                    )
                    .await
            } else if let Some(root) = root {
                commit.commit_root_read_acquisition(
                    request,
                    &outcome,
                    descriptor,
                    root,
                    &locked.scope,
                )
            } else {
                commit.commit_bound_readonly_tool_receipt(
                    request,
                    &outcome,
                    descriptor,
                    &locked.scope,
                )
            }
        } else if host_process_terminal {
            commit
                .commit_host_process_closure(request, descriptor, &outcome)
                .await
        } else {
            commit.commit_tool_effect(request, descriptor, &outcome)
        };
        committed.map_err(|error| {
            ToolError::new(format!(
                "tool effect completed but durable receipt failed: {error}"
            ))
        })?;
        drop(locked);
        execution
    }

    pub(super) async fn execute_single_tool(
        &self,
        task: &crate::governed_tool_plan::GovernedToolPlanTask,
        plan_id: &str,
        plan_revision: u64,
        input: &str,
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        retained_admission: &mut Option<crate::ToolExecutionAdmission>,
    ) -> Result<ConversationMessage, RuntimeError>
    where
        C: Sync,
    {
        self.execute_single_tool_at(
            task,
            plan_id,
            plan_revision,
            input,
            prompter,
            iterations,
            retained_admission,
            None,
            None,
        )
        .await
    }

    #[inline(never)]
    fn execute_single_tool_at<'tool>(
        &'tool self,
        task: &'tool crate::governed_tool_plan::GovernedToolPlanTask,
        plan_id: &'tool str,
        plan_revision: u64,
        input: &'tool str,
        prompter: &'tool crate::permissions::SharedPrompter,
        iterations: usize,
        retained_admission: &'tool mut Option<crate::ToolExecutionAdmission>,
        caller_owner: Option<&'tool (harness_contract::projection::RuntimeActivityBinding, u32)>,
        preflight_refusals: Option<&'tool Mutex<Vec<(String, String)>>>,
    ) -> GovernedToolFuture<'tool, Result<ConversationMessage, RuntimeError>>
    where
        C: Sync,
    {
        boxed_tool_future(|| async move {
            let tool_use_id = task.tool_call_id.as_str();
            let tool_name = task.tool_name.as_str();
            let refusal = self.blocked_repeated_call(tool_name, tool_use_id, input, iterations);
            if let Some(refusal) = refusal {
                self.note_refusal_ignored(tool_name, input);
                return Ok(refusal);
            }
            let (
                pre_hook_result,
                effective_input,
                model_delivery_requirement,
                tool_timeout,
                authorization_decision,
            ) = boxed_tool_future(|| async {
                let (
                    pre_hook_result,
                    effective_input,
                    model_delivery_requirement,
                    mut permission_context,
                ) = self.prepare_tool_call(tool_name, input);
                let effective_value = harness_contract::step::parse_strict_json(&effective_input)
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                if effective_value != task.invocation.intent.normalized_input {
                    return Err(RuntimeError::new("effective_input_requires_replan"));
                }
                if pre_hook_result.is_cancelled() {
                    permission_context = PermissionContext::new(
                        Some(crate::permissions::PermissionOverride::Deny),
                        Some(format!("PreToolUse hook cancelled tool `{tool_name}`")),
                    );
                } else if pre_hook_result.is_failed() {
                    let hook_msgs = pre_hook_result.messages().join("; ");
                    permission_context = PermissionContext::new(
                        Some(crate::permissions::PermissionOverride::Deny),
                        Some(if hook_msgs.is_empty() {
                            format!("PreToolUse hook failed for tool `{tool_name}`")
                        } else {
                            format!("PreToolUse hook failed for tool `{tool_name}`: {hook_msgs}")
                        }),
                    );
                } else if pre_hook_result.is_denied() {
                    permission_context = PermissionContext::new(
                        Some(crate::permissions::PermissionOverride::Deny),
                        Some(format!("PreToolUse hook denied tool `{tool_name}`")),
                    );
                }
                let profile_timeout =
                    Duration::from_secs(task.safety_category.default_timeout_secs());
                let tool_timeout = self
                    .tool_timeout
                    .map_or(profile_timeout, |timeout| timeout.min(profile_timeout));
                let authorization_id = format!(
                    "{}:{plan_id}:{plan_revision}:{tool_use_id}:{iterations}",
                    self.session_id()
                );
                let authorization_decision = self
                    .negotiate_tool_authorization(
                        &task.effect,
                        &effective_input,
                        authorization_id,
                        permission_context,
                        tool_timeout.as_secs(),
                        prompter,
                    )
                    .await?;

                Ok::<_, RuntimeError>((
                    pre_hook_result,
                    effective_input,
                    model_delivery_requirement,
                    tool_timeout,
                    authorization_decision,
                ))
            })
            .await?;

            match authorization_decision {
                ToolAuthorizationDecision::Authorized(authorization) => {
                    let (execution, invocation_record, start, tname_for_err) = boxed_tool_future(|| async {
                let execution_policy = self.permission_policy.execution_policy_control().snapshot();
                if execution_policy.revision != authorization.authorization.policy_revision {
                    return Err(RuntimeError::new(format!(
                        "session_policy_revision_stale: authorization rev {} current rev {}; replan before tool admission",
                        authorization.authorization.policy_revision, execution_policy.revision
                    )));
                }
                let executor_owns_durable_effect =
                    self.tool_executor.owns_durable_tool_effect(tool_name);
                let root = self.root_effect_context();
                let activity = self
                    .cowd_bus
                    .as_ref()
                    .and_then(|bus| bus.current_activity_binding());
                let effect_parent = if executor_owns_durable_effect {
                    None
                } else {
                    checked_delegated_effect_parent(
                        self.tool_executor.durable_tool_effect_parent(),
                        task.step_binding
                            .as_ref()
                            .and_then(|binding| binding.snapshot.identity.run_ref.as_deref()),
                        self.execution_identity.as_ref(),
                        activity.as_ref(),
                        self.session_id(),
                    )?
                };
                if effect_parent.is_some() && root.is_some() {
                    return Err(RuntimeError::new(
                        "delegated tool effect cannot use root effect context",
                    ));
                }
                let root_owner = if !executor_owns_durable_effect && root.is_some() {
                    Some(match caller_owner {
                        Some(owner) => owner.clone(),
                        None => self.root_effect_owner()?,
                    })
                } else {
                    None
                };
                let invocation_record = self
                    .start_tool_invocation_record(
                        tool_use_id,
                        tool_name,
                        &effective_input,
                        iterations,
                    )
                    .with_governed_plan(plan_id, plan_revision);
                self.verify_session_execution_fence(
                    crate::SessionExecutionFencePhase::ToolExecution,
                )
                .await?;
                self.record_tool_invocation_event(
                    &invocation_record,
                    "tool.invocation.started",
                    self.session_head().await.message_count,
                    None,
                );
                self.record_tool_started(iterations, tool_name);
                if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
                    metrics.observe_invocation(tool_name);
                }
                if let Some(callback) = &self.tool_callback {
                    let preview: String = effective_input.chars().take(200).collect();
                    callback.on_tool_start(tool_use_id, tool_name, &preview);
                }

                let start = Instant::now();
                let tname = tool_name.to_string();
                let tname_for_err = tname.clone();
                let tinput = effective_input.clone();
                let provider_invocation_id = tool_use_id.to_string();
                let tool_exec = Arc::clone(&self.tool_executor);
                let evidence_sandbox = self.tool_output_sandbox.clone();
                let is_evidence_retrieve = tool_name == "evidence_retrieve";
                let demand = task.resource_demand.clone();
                let plane = Arc::clone(&self.tool_execution_plane);
                let executor_owns_admission =
                    self.tool_executor.owns_tool_resource_admission(tool_name);
                let effect_request = crate::RuntimeToolExecutionRequest {
                    compiled_invocation: Some(task.invocation.clone()),
                    step_binding: task.step_binding.clone(),
                    governed_plan_id: plan_id.to_string(),
                    governed_plan_revision: plan_revision,
                    observation_wave_sequence: u64::try_from(iterations).unwrap_or(u64::MAX).max(1),
                    idempotency_key: task
                        .step_binding
                        .as_ref()
                        .map(crate::step_snapshot::StepCallBinding::effect_idempotency_key)
                        .unwrap_or_else(|| {
                            format!(
                                "{}:{plan_id}:{plan_revision}:{tool_use_id}:{iterations}",
                                self.session_id()
                            )
                        }),
                    tool_use_id: tool_use_id.to_string(),
                    tool_name: tool_name.to_string(),
                    input: effective_input.clone(),
                    category: task.safety_category,
                    authorization: Some(authorization.authorization.clone()),
                    session_id: Some(self.session_id().to_string()),
                    sandbox_posture: execution_policy.sandbox_posture,
                    policy_revision: authorization.authorization.policy_revision,
                    authorized_scopes: vec![format!("session:{}", self.session_id())],
                    memory_context: Some(self.memory_turn_context()),
                    reality_context: self.reality_data_lease(),
                    model_lease: None,
                    parent_execution: effect_parent.as_ref().map(|(parent, _)| parent.clone()),
                    parent_execution_attempt: effect_parent.as_ref().map(|(_, attempt)| *attempt),
                    execution_decision: None,
                    evaluation_isolated: false,
                    managed_invocation: None,
                    tool_progress: crate::ToolProgressSink(self.cowd_bus.as_ref().map(|bus| {
                        let bus = bus.clone();
                        let id = tool_use_id.to_string();
                        let name = tool_name.to_string();
                        let callback: std::sync::Arc<dyn Fn(&str) + Send + Sync> =
                            std::sync::Arc::new(move |progress| {
                                bus.emit_tool_progress(&id, &name, progress);
                            });
                        callback
                    })),
                };
                let effect_commit = (!executor_owns_durable_effect)
                    .then(|| {
                        self.runtime_event_store.as_ref().map(|store| {
                            let mut commit =
                                crate::execution_core::graph::ExecutionCommitService::new(
                                    Arc::clone(store),
                                );
                            if let Some(artifacts) = self.artifact_store.as_ref() {
                                commit = commit.with_artifact_store(Arc::clone(artifacts));
                            }
                            commit
                        })
                    })
                    .flatten();
                let effect_state: Result<
                    crate::execution_core::graph::ToolEffectState,
                    crate::execution_core::graph::ExecutionCommitError,
                > = match (executor_owns_durable_effect, effect_commit.as_ref()) {
                    (true, _) => Ok(crate::execution_core::graph::ToolEffectState::NotRequired),
                    (false, Some(commit)) => {
                        if let Some(outcome) = commit
                            .completed_tool_effect(&effect_request, &task.effect)
                            .map_err(|error| RuntimeError::new(error.to_string()))?
                        {
                            Ok(crate::execution_core::graph::ToolEffectState::Completed(
                                outcome,
                            ))
                        } else {
                            Ok(crate::execution_core::graph::ToolEffectState::Fresh)
                        }
                    }
                    (false, None) => {
                        return Err(RuntimeError::new(
                            "tool execution requires the durable Runtime effect ledger",
                        ));
                    }
                };
                let execution = match effect_state {
                    Err(error) => Ok(Err(ToolError::new(error.to_string()))),
                    Ok(crate::execution_core::graph::ToolEffectState::Completed(outcome)) => {
                        Ok(replay_tool_output(
                            crate::bound_tool_batch::retained_replay_outcome(outcome),
                        ))
                    }
                    Ok(crate::execution_core::graph::ToolEffectState::Uncertain) => {
                        return Err(RuntimeError::new(format!(
                            "tool effect `{}` is uncertain; non-idempotent execution was not replayed",
                            effect_request.idempotency_key
                        )));
                    }
                    Ok(
                        crate::execution_core::graph::ToolEffectState::Fresh
                        | crate::execution_core::graph::ToolEffectState::NotRequired,
                    ) => {
                        let step_binding = effect_request.step_binding.clone();
                        let effect_commit = effect_commit.as_ref();
                        let effect_request = &effect_request;
                        let root = root.as_ref();
                        let root_owner = root_owner.as_ref();
                        let operation = boxed_tool_future(|| async move {
                            let execute = || boxed_tool_future(|| async move {
                                if is_evidence_retrieve {
                                    if let Ok(output) = retrieve_tool_evidence_from_sandbox(
                                        evidence_sandbox.as_ref(),
                                        &tinput,
                                    ) {
                                        return Ok(
                                        harness_contract::context::ToolOutputDraft::bounded_inline(
                                            output,
                                        ),
                                    );
                                    }
                                    // Small outputs are intentionally not duplicated in the
                                    // in-memory search index, while every completed invocation
                                    // still exposes its durable `tool://` ArtifactStore ref.
                                    // Let the authorized Runtime ToolHost resolve that source of
                                    // truth instead of turning an index miss into a false tool
                                    // failure. Scope and Session checks remain in that host.
                                }
                                if matches!(tname.as_str(), "tool_search" | "runtime_capabilities")
                                {
                                    tool_exec
                                        .execute_invocation_output(
                                            &provider_invocation_id,
                                            &tname,
                                            &tinput,
                                        )
                                        .await
                                } else {
                                    tool_exec
                                        .execute_step_authorized_invocation_output(
                                            step_binding.as_ref(),
                                            &provider_invocation_id,
                                            &authorization.authorization,
                                            &tname,
                                            &tinput,
                                        )
                                        .await
                                }
                            });
                            if executor_owns_durable_effect {
                                execute().await
                            } else {
                                let effect_commit = effect_commit.ok_or_else(|| {
                                    ToolError::new("tool execution lost its durable ledger")
                                })?;
                                boxed_tool_future(|| self.execute_scoped_root_effect(
                                    effect_commit,
                                    effect_request,
                                    &task.effect,
                                    root,
                                    root_owner,
                                    executor_owns_admission,
                                    execute,
                                ))
                                .await
                            }
                        });
                        let (execution, admission) = if executor_owns_admission {
                            (Ok(operation.await), None)
                        } else {
                            plane
                                .execute_async_classified_retained(
                                    &demand,
                                    Some(tool_timeout),
                                    self.execution_service_class,
                                    Some(self.execution_service_class),
                                    Some(self.session_id()),
                                    operation,
                                )
                                .await
                        };
                        *retained_admission = admission;
                        execution
                    }
                };

                    Ok::<_, RuntimeError>((execution, invocation_record, start, tname_for_err))
                }).await?;
                    boxed_tool_future(|| async {
                let (output_draft, mut is_error, mut failure_kind) = match execution {
                    Ok(Ok(output)) => model_tool_output_feedback(tool_name, output),
                    Ok(Err(error)) => {
                        if let Some(refusals) = preflight_refusals {
                            if let Some(reason) = error.pre_effect_refusal_for(
                                tool_use_id,
                                tool_name,
                                &effective_input,
                                task.step_binding.as_ref(),
                            ) {
                                refusals
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .push((tool_use_id.to_string(), reason.to_string()));
                            }
                        }
                        (
                            harness_contract::context::ToolOutputDraft::bounded_inline(
                                error.to_string(),
                            ),
                            true,
                            Some(ToolFailureKind::ExecutionError),
                        )
                    }
                    Err(crate::ToolExecutionPlaneError::TimedOut(_)) => {
                        tracing::warn!(tool = %tname_for_err, timeout_secs = tool_timeout.as_secs(), "tool execution waiter timed out; started operation remains fenced");
                        (
                            harness_contract::context::ToolOutputDraft::bounded_inline(format!(
                                "tool `{tname_for_err}` timed out after {tool_timeout:?}"
                            )),
                            true,
                            Some(ToolFailureKind::Timeout),
                        )
                    }
                    Err(crate::ToolExecutionPlaneError::Panicked) => (
                        harness_contract::context::ToolOutputDraft::bounded_inline(
                            "tool execution panicked",
                        ),
                        true,
                        Some(ToolFailureKind::Panic),
                    ),
                    Err(error) => (
                        harness_contract::context::ToolOutputDraft::bounded_inline(
                            error.to_string(),
                        ),
                        true,
                        Some(ToolFailureKind::ExecutionError),
                    ),
                };
                let output = output_draft.model_text().to_string();
                let elapsed_ms = start.elapsed().as_millis() as u64;
                self.hook_runner
                    .fire_post_tool(tool_name, &output, is_error, elapsed_ms);

                if let Some(callback) = &self.tool_callback {
                    let summary: String = output.chars().take(500).collect();
                    let exit_code = if is_error { Some(1) } else { Some(0) };
                    callback.on_tool_complete(tool_use_id, tool_name, &summary, exit_code);
                }

                let post_hook_result = if is_error {
                    self.run_post_tool_use_failure_hook(tool_name, &effective_input, &output)
                } else {
                    self.run_post_tool_use_hook(tool_name, &effective_input, &output, false)
                };
                if post_hook_result.is_denied()
                    || post_hook_result.is_failed()
                    || post_hook_result.is_cancelled()
                {
                    is_error = true;
                    if failure_kind.is_none() {
                        failure_kind = Some(ToolFailureKind::HookDenied);
                    }
                }

                let elapsed_ms = start.elapsed().as_millis() as u64;
                if let Some(cowd) = self.cowd_bus() {
                    cowd.emit(crate::cowd_event::CowdEvent::ToolExecuted {
                        name: tool_name.to_string(),
                        duration_ms: elapsed_ms,
                    });
                }

                // T36: Truncate oversized tool results before storing.
                // Append hook feedback messages to the tool output.
                let tool_search_activated = tool_name == "tool_search"
                    && !is_error
                    && self
                        .activate_tool_discovery(&output)
                        .is_some_and(|receipt| receipt.activated_ids().next().is_some());
                let original_read_output =
                    matches!(tool_name, "read_file" | "read_ref" | "evidence_retrieve")
                        .then(|| output.clone());
                let mut combined = if tool_name == "runtime_capabilities" && !is_error {
                    self.project_runtime_capabilities_for_model(&output)
                } else {
                    output
                };
                if tool_search_activated {
                    combined.push_str(
                        "\n\nThe discovered tools are active on the immediately following \
                         automatic model request in this same turn. Continue the current task \
                         and invoke the relevant activated tool directly; do not ask the user \
                         to resend solely because activation just completed.",
                    );
                }
                for msg in pre_hook_result.messages() {
                    combined.push_str(&format!("\n{msg}"));
                }
                for msg in post_hook_result.messages() {
                    combined.push_str(&format!("\n{msg}"));
                }
                self.append_repetition_diagnosis(
                    tool_name,
                    invocation_record.input_hash.as_str(),
                    &invocation_record.input_refs,
                    is_error,
                    failure_kind.unwrap_or(ToolFailureKind::Unknown),
                    &mut combined,
                );
                let completed_record = if is_error {
                    invocation_record.failed_with_output_policy(
                        failure_kind.unwrap_or(ToolFailureKind::Unknown),
                        &combined,
                        now_ms(),
                        DEFAULT_OUTPUT_REF_MIN_LINES,
                    )
                } else {
                    invocation_record.completed_with_output_policy(
                        &combined,
                        now_ms(),
                        DEFAULT_OUTPUT_REF_MIN_LINES,
                    )
                };
                let prepared_vision = prepared_vision_payload(tool_name, &combined, is_error);
                let indexable_output = prepared_vision
                    .as_ref()
                    .map(vision_index_summary)
                    .unwrap_or_else(|| combined.clone());
                let (raw_ref, raw_access) = self
                    .record_tool_output_evidence(
                        tool_use_id,
                        tool_name,
                        &completed_record.input_hash,
                        &output_draft,
                        original_read_output.as_deref().unwrap_or(&combined),
                        is_error,
                        elapsed_ms,
                        None,
                    )
                    .await?;
                self.maybe_index_tool_output(
                    raw_ref.id(),
                    tool_name,
                    &indexable_output,
                    Some(&raw_access),
                );
                let completed_record =
                    completed_record.with_full_output_ref(format!("tool://{}", raw_ref.id()));
                let mut model_receipt = self.tool_model_receipt(
                    tool_name,
                    &combined,
                    is_error,
                    &raw_ref,
                    Some(&raw_access),
                    &model_delivery_requirement,
                    &effective_input,
                );
                if let Some(payload) = prepared_vision.as_ref() {
                    model_receipt.summary = vision_tool_model_receipt(payload, &raw_ref);
                    model_receipt.receipt_tokens =
                        crate::context_ledger::estimate_text_tokens(&model_receipt.summary);
                    model_receipt.omitted_tokens = model_receipt.raw_tokens.max(1);
                    model_receipt.truncated = true;
                }
                self.record_generated_model_receipt(
                    tool_use_id,
                    tool_name,
                    &model_delivery_requirement,
                    &raw_ref,
                    &model_receipt,
                    is_error,
                    original_read_output.as_deref().unwrap_or(&combined),
                )?;
                let audit_projection =
                    crate::context_evidence::audit_projection(&model_receipt, Some(&raw_access));
                self.push_turn_evidence_audit(audit_projection);
                let model_summary = agentic_action_body(&combined).unwrap_or(model_receipt.summary);
                let output_envelope = harness_contract::context::ToolOutputEnvelope {
                    artifact_ref: Some(harness_contract::context::ArtifactRef::durable(
                        raw_access.retrieval_selector.clone(),
                        raw_access.sha256.clone(),
                        raw_access.bytes,
                        raw_access.media_type.clone(),
                        raw_access.visibility_scope.clone(),
                    )),
                    evidence_ref: Some(raw_access.clone()),
                    receipt: completed_record
                        .full_output_ref
                        .clone()
                        .unwrap_or_else(|| format!("tool://{}", raw_ref.id())),
                };
                self.push_turn_tool_observation(
                    ToolObservation::new(
                        tool_name.to_string(),
                        completed_record.invocation_id.clone(),
                        raw_ref,
                        model_summary.clone(),
                    )
                    .with_output_envelope(output_envelope),
                );
                let result = ConversationMessage::tool_result(
                    tool_use_id.to_string(),
                    tool_name.to_string(),
                    model_summary,
                    is_error,
                );
                self.session
                    .write()
                    .await
                    .push_message(result.clone())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                let sequence = self.session_head().await.message_count.wrapping_sub(1);
                self.record_message_event(&result, sequence);
                if let Some(payload) = prepared_vision {
                    let image_message = vision_user_message(&payload);
                    self.session
                        .write()
                        .await
                        .push_message(image_message.clone())
                        .map_err(|error| RuntimeError::new(error.to_string()))?;
                    self.record_message_event(
                        &image_message,
                        self.session_head().await.message_count.wrapping_sub(1),
                    );
                }
                self.record_tool_invocation_event(
                    &completed_record,
                    if is_error {
                        "tool.invocation.failed"
                    } else {
                        "tool.invocation.completed"
                    },
                    self.session_head().await.message_count.wrapping_sub(1),
                    Some(&raw_access),
                );
                self.record_tool_finished(iterations, &result);
                Ok(result)
                }).await
                }
                ToolAuthorizationDecision::Gap { assessment, .. } => {
                    let gap = assessment.gap.as_ref();
                    let reason = gap.map_or_else(
                        || "capability authorization was not granted".to_string(),
                        |gap| gap.reason.clone(),
                    );
                    let failure_kind = if reason.starts_with("PreToolUse hook") {
                        ToolFailureKind::HookDenied
                    } else {
                        ToolFailureKind::PermissionDenied
                    };
                    self.record_tool_invocation_denied(
                        tool_use_id,
                        tool_name,
                        &effective_input,
                        iterations,
                        failure_kind,
                        &reason,
                    );
                    let first_recovery = gap.is_some_and(|gap| gap.recoverable);
                    let payload = serde_json::json!({
                    "kind": "capability_gap",
                    "assessment_id": assessment.assessment_id,
                    "path": assessment.path,
                    "gap": assessment.gap,
                    "controlled_recovery_available": first_recovery,
                    "instruction": if first_recovery {
                        "Use one listed safe alternative or revise the plan with existing capabilities. Do not repeat the same denied action without new evidence or approval."
                    } else {
                        "The same capability gap is closed for this turn. Preserve evidence and report the limitation without retrying the denied action."
                    },
                })
                .to_string();
                    let denied = ConversationMessage::tool_result(
                        tool_use_id.to_string(),
                        tool_name.to_string(),
                        payload,
                        !first_recovery,
                    );
                    self.session
                        .write()
                        .await
                        .push_message(denied.clone())
                        .map_err(|error| RuntimeError::new(error.to_string()))?;
                    let sequence = self.session_head().await.message_count.wrapping_sub(1);
                    self.record_message_event(&denied, sequence);
                    Ok(denied)
                }
            }
        })
    }

    pub(super) fn collect_tool_result_message(
        &self,
        result_msg: ConversationMessage,
    ) -> (String, (ConversationMessage, Option<String>)) {
        let (msg_id, tool_name) = extract_tool_info(&result_msg);
        let inject = self.turn_callback.as_ref().and_then(|callback| {
            let output = result_msg
                .blocks
                .first()
                .and_then(|block| match block {
                    ContentBlock::ToolResult { output, .. } => Some(output.as_str()),
                    _ => None,
                })
                .unwrap_or("");
            (callback.on_tool_result)(&tool_name, output)
        });
        (msg_id, (result_msg, inject))
    }

    /// Ingest an outcome already executed by the graph-owned tool host.
    /// The graph remains responsible for publication; this method persists
    /// raw evidence, updates context governance, and applies Runtime-owned
    /// capability discovery before the next model request.
    #[cfg(test)]
    pub(crate) async fn prepare_governed_tool_result(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
    ) -> Result<ConversationMessage, RuntimeError> {
        self.prepare_governed_tool_result_with_invocation(
            tool_use_id,
            tool_name,
            input,
            output,
            is_error,
            None,
        )
        .await
    }

    pub(crate) async fn prepare_governed_tool_result_with_invocation(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
        invocation: Option<ToolInvocationRecord>,
    ) -> Result<ConversationMessage, RuntimeError> {
        if invocation.as_ref().is_some_and(|record| {
            record.tool_call_id != tool_use_id
                || record.tool_name != tool_name
                || record.session_id != self.session_id()
                || record.input_hash != crate::tool_invocation::stable_hash(input)
        }) {
            return Err(RuntimeError::new(
                "prepared Tool invocation crosses its original call/input owner",
            ));
        }
        if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
            metrics.observe_invocation(tool_name);
        }
        let tool_search_activated = tool_name == "tool_search"
            && !is_error
            && self
                .activate_tool_discovery(output)
                .is_some_and(|receipt| receipt.activated_ids().next().is_some());
        let input_hash = format!(
            "{:016x}",
            model_protocol::fingerprint::stable_hash_bytes(input.as_bytes())
        );
        let source_evidence_ref = format!("runtime-tool:{tool_use_id}");
        let (raw_ref, raw_access) = self
            .record_tool_raw_evidence(
                tool_use_id,
                tool_name,
                &input_hash,
                output,
                is_error,
                0,
                Some(&source_evidence_ref),
            )
            .await?;
        if let Some(mut terminal) = invocation {
            let sequence = self.session_head_blocking().message_count;
            terminal.session_id = self.session_id().to_string();
            terminal.turn_index = sequence;
            terminal = terminal.with_full_output_ref(format!("tool://{}", raw_ref.id()));
            self.record_tool_invocation_event(
                &terminal.started_fact(),
                "tool.invocation.started",
                sequence,
                None,
            );
            let terminal_kind = match terminal.status.as_str() {
                "completed" => "tool.invocation.completed",
                "denied" => "tool.invocation.denied",
                _ => "tool.invocation.failed",
            };
            self.record_tool_invocation_event(
                &terminal,
                terminal_kind,
                sequence,
                Some(&raw_access),
            );
        }
        self.maybe_index_tool_output(raw_ref.id(), tool_name, output, Some(&raw_access));
        let delivery_requirement = self
            .tool_executor
            .model_delivery_requirement(tool_name, input);
        let receipt = self.tool_model_receipt(
            tool_name,
            output,
            is_error,
            &raw_ref,
            Some(&raw_access),
            &delivery_requirement,
            input,
        );
        self.record_generated_model_receipt(
            tool_use_id,
            tool_name,
            &delivery_requirement,
            &raw_ref,
            &receipt,
            is_error,
            output,
        )?;
        self.push_turn_evidence_audit(crate::context_evidence::audit_projection(
            &receipt,
            Some(&raw_access),
        ));
        let mut summary = receipt.summary;
        if tool_search_activated {
            summary.push_str(
                "\n\nThe discovered tools are active on the immediately following automatic \
                 model request in this same turn. Continue the current task and invoke the \
                 relevant activated tool directly; do not ask the user to resend solely because \
                 activation just completed.",
            );
        }
        let output_envelope = harness_contract::context::ToolOutputEnvelope {
            artifact_ref: Some(harness_contract::context::ArtifactRef::durable(
                raw_access.retrieval_selector.clone(),
                raw_access.sha256.clone(),
                raw_access.bytes,
                raw_access.media_type.clone(),
                raw_access.visibility_scope.clone(),
            )),
            evidence_ref: Some(raw_access),
            receipt: format!("tool://{}", raw_ref.id()),
        };
        self.push_turn_tool_observation(
            ToolObservation::new(
                tool_name.to_string(),
                tool_use_id.to_string(),
                raw_ref,
                summary.clone(),
            )
            .with_output_envelope(output_envelope),
        );
        Ok(ConversationMessage::tool_result(
            tool_use_id.to_string(),
            tool_name.to_string(),
            summary,
            is_error,
        ))
    }

    /// Resolve what one tool call runs under: its pre-hook outcome, the input
    /// that hook settled on, the delivery requirement that follows from it, and
    /// the permission context the hook asked for.
    ///
    /// Extracted so the refusal check can stay first in the caller without
    /// pushing that function further past its structural limit.
    fn prepare_tool_call(
        &self,
        tool_name: &str,
        input: &str,
    ) -> (
        HookRunResult,
        String,
        crate::conversation::ToolModelDeliveryRequirement,
        PermissionContext,
    ) {
        let pre_hook_result = self.run_pre_tool_use_hook(tool_name, input);
        let effective_input = pre_hook_result
            .updated_input()
            .map_or_else(|| input.to_string(), ToOwned::to_owned);
        let model_delivery_requirement = self
            .tool_executor
            .model_delivery_requirement(tool_name, &effective_input);
        let permission_context = PermissionContext::new(
            pre_hook_result.permission_override(),
            pre_hook_result.permission_reason().map(ToOwned::to_owned),
        );
        (
            pre_hook_result,
            effective_input,
            model_delivery_requirement,
            permission_context,
        )
    }

    /// The turn this failure or refusal belongs to.
    ///
    /// The ledger is keyed by turn, so the identity has to be settled before a
    /// failure is recorded or a dispatch is refused. Resolving it in one place
    /// keeps both paths on the same identity.
    pub(crate) fn active_turn_id(&self) -> String {
        self.session_input_stream
            .active_turn_id()
            .map(|turn| turn.as_str().to_string())
            .unwrap_or_else(|| self.session_id().to_string())
    }

    pub(crate) fn active_repetition_scope(&self) -> crate::repetition_guard::RepetitionScope {
        self.runtime_event_store.as_ref().map_or_else(
            || {
                crate::repetition_guard::RepetitionScope::turn(
                    self.session_id(),
                    &self.active_turn_id(),
                )
            },
            |store| store.repetition_scope(self.session_id(), &self.active_turn_id()),
        )
    }

    /// Append the repetition diagnosis when this exact call has already failed.
    ///
    /// Extracted from the tool path so it does not lengthen a function that is
    /// already over the structural limit, and because resolving the turn identity
    /// is worth reading on its own: the ledger is keyed by turn, so the identity
    /// has to be settled before the failure is recorded.
    /// Refuse a call that has already failed deterministically this many times.
    ///
    /// A diagnosis is advice the model can ignore; a refusal is what ends the loop.
    /// One live run re-sent one review call one hundred and twenty-two times against
    /// a graph it could not name, paying for every attempt, because nothing refused
    /// it. The input fingerprint is the same one the failure ledger keys on, so a
    /// refused call is exactly a recorded one. Transient failures never count, so a
    /// retry that can legitimately succeed is never refused.
    ///
    /// The refusal is also recorded as evidence. A refusal that only returns a tool
    /// result leaves the report unable to reconcile with the ledger: the model was
    /// answered, but no invocation record exists for the answer.
    ///
    /// A refusal the model answers by issuing the same call again is a refusal it
    /// is not reading. Run one31 read the same file seven times: the Runtime
    /// refused the fifth, sixth and seventh with guidance, and the model issued
    /// them anyway. Past the ignored-refusal bound the next model step is
    /// text-only, so the model must conclude rather than re-issue - the same
    /// one-request override the judge and recovery paths already use.
    fn note_refusal_ignored(&self, tool_name: &str, input: &str) {
        let Some(ignored_bound) = ignored_refusal_stop_bound() else {
            return;
        };
        let input_hash = crate::tool_invocation::stable_hash(input);
        let repeated = crate::repetition_guard::repeated_call_count(
            &self.active_repetition_scope(),
            tool_name,
            &input_hash,
        );
        let block_bound = crate::repetition_guard::repeated_call_block_bound_for(tool_name, input);
        if repeated < block_bound + 1 + ignored_bound {
            return;
        }
        // The turn state is not reachable from here - the tool plane runs on
        // ConversationRuntime, the state lives with the turn backend - so the
        // escalation is recorded turn-scoped in the guard the refusal path
        // already keys on, and the model-step preparation consumes it.
        crate::repetition_guard::mark_refusal_ignored(&self.active_repetition_scope());
        tracing::warn!(
            %tool_name,
            repeated,
            block_bound,
            ignored_bound,
            "the model kept issuing a call Runtime had already refused; the next step is text-only so it must conclude"
        );
    }

    fn blocked_repeated_call(
        &self,
        tool_name: &str,
        tool_use_id: &str,
        input: &str,
        iterations: usize,
    ) -> Option<ConversationMessage> {
        let turn_id = self.active_turn_id();
        let repetition_scope = self.active_repetition_scope();
        let input_hash = crate::tool_invocation::stable_hash(input);
        // A call that keeps succeeding is still a call that keeps producing the
        // same bytes. The failure ledger cannot see it, and the cumulative counter
        // only diagnoses - run one23 was diagnosed eight times and issued the same
        // `read_ref` twenty-one times. Past the block bound the call is not
        // dispatched at all, which is the only mechanism here that has ever
        // actually stopped a loop.
        let repeated =
            crate::repetition_guard::repeated_call_count(&repetition_scope, tool_name, &input_hash);
        let repeated_refusal =
            repeated >= crate::repetition_guard::repeated_call_block_bound_for(tool_name, input);
        // Measure the call pattern, but do not refuse on it alone.
        //
        // One live run read eight *different* artifacts in rotation, each input
        // staying under its own bound, so no per-input breaker fired while the
        // turn stood still - the stagnation monitor stopped the run because the
        // framework's own guard would not. A tool dominating the recent window is
        // the signal that case was missing.
        //
        // It is deliberately not a bound. An independent reviewer reading nine
        // artifacts in a row produces the same dominance while doing exactly its
        // job, and refusing that costs a real review. Dominance says the turn is
        // cycling; only the absence of progress says the cycling is empty, and
        // that verdict belongs to the guard that already measures progress. This
        // records the measurement so that guard - and the operator - can see it.
        let pattern_dominant =
            crate::repetition_guard::observe_call_pattern(&repetition_scope, tool_name);
        if pattern_dominant {
            self.append_execution_runtime_event(
                RuntimeEventScope::Tool,
                "tool.call_pattern_dominant",
                Some("observed".to_string()),
                vec![
                    RuntimeEventRef {
                        kind: "tool_call".to_string(),
                        id: tool_use_id.to_string(),
                    },
                    RuntimeEventRef {
                        kind: "tool".to_string(),
                        id: tool_name.to_string(),
                    },
                ],
                serde_json::json!({
                    "tool_name": tool_name,
                    "turn_id": turn_id,
                    "input_hash": input_hash,
                    "identical_repeats": repeated,
                    "note": "one tool dominates the recent call window; progress, not dominance, decides whether this is a loop",
                }),
            );
        }
        let Some(refusal) =
            crate::repetition_guard::refusal_for(&repetition_scope, tool_name, &input_hash)
        else {
            if !repeated_refusal {
                return None;
            }
            tracing::warn!(
                %tool_name, %turn_id, repeated, pattern_dominant,
                "dispatch refused for a repeatedly identical successful call"
            );
            // The way out has to match the tool. Telling a model that polled
            // `state_inspect` thirteen times to "open the durable receipt" sends it
            // somewhere it does not want to go - run one28 was given exactly that
            // advice and re-issued the call thirteen times anyway. A refusal that
            // names no reachable next step is not a bound; it is a wall.
            let way_out = match tool_name {
                "read_file" | "read_ref" | "glob_search" | "grep_search" | "workspace_snapshot" => {
                    "The bytes are already committed: open the durable receipt with \
                     `evidence_retrieve` (same bytes, Runtime-bound hash) instead of issuing the \
                     call again."
                }
                "state_inspect" => {
                    "Reuse the existing state observation. Wait only through the protocol allowed by your current role, or take a different valid action."
                }
                _ => {
                    crate::repetition_guard::SCOPED_RECOVERY_GUIDANCE
                }
            };
            let reason = format!(
                "Runtime refused to dispatch this call again: the identical `{tool_name}` call has \
                 already been issued {repeated} times in this turn and each time returned the same \
                 result, so issuing it again cannot advance the objective. {way_out}"
            );
            return Some(self.repeated_call_refusal_message(
                tool_use_id,
                tool_name,
                input,
                &reason,
            ));
        };
        tracing::warn!(%tool_name, %turn_id, "dispatch refused for a repeatedly failing call");
        let reason = format!(
            "Runtime refused to dispatch this call again: it has already failed \
             deterministically {} times in this turn, so retrying it cannot produce a \
             different result. {}",
            refusal.count,
            crate::repetition_guard::SCOPED_RECOVERY_GUIDANCE
        );
        // The recorded cause is the recovery path, not bookkeeping: when the
        // deterministic failure was an unresolvable evidence reference it
        // names the canonical durable receipt, so the model can correct the
        // reference instead of guessing a third variant of it.
        let reason = if refusal.cause_signature.trim().is_empty() {
            reason
        } else {
            format!("{reason} Recorded cause: {}", refusal.cause_signature)
        };
        let record = self
            .start_tool_invocation_record(tool_use_id, tool_name, input, iterations)
            .refused(&reason, now_ms());
        let refs = vec![
            RuntimeEventRef {
                kind: "tool_invocation".to_string(),
                id: record.invocation_id.clone(),
            },
            RuntimeEventRef {
                kind: "tool_call".to_string(),
                id: record.tool_call_id.clone(),
            },
        ];
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            "tool.invocation.refused",
            Some(record.status.as_str().to_string()),
            refs,
            serde_json::json!({
                "status": record.status.as_str().to_string(),
                "invocation_id": record.invocation_id,
                "tool_call_id": record.tool_call_id,
                "tool_name": record.tool_name,
                "turn_id": turn_id,
                "input_hash": record.input_hash,
                "cause_signature": refusal.cause_signature,
                "count": refusal.count,
                "state_derived": refusal.state_derived,
            }),
        );
        Some(ConversationMessage::tool_result(
            tool_use_id.to_string(),
            tool_name.to_string(),
            reason,
            true,
        ))
    }

    /// Refuse a repeatedly identical *successful* call, recording the refusal the
    /// same way the failure ledger does.
    ///
    /// The event and the tool result both matter. The event is what lets an
    /// operator see afterwards that Runtime stopped a loop and why; the tool
    /// result is what the model reads, and it has to name the durable receipt
    /// rather than only saying "do not repeat" - a model told to change its input
    /// without being told where the bytes already are has nowhere to go.
    fn repeated_call_refusal_message(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        reason: &str,
    ) -> ConversationMessage {
        let turn_id = self.active_turn_id();
        // The real tool name and input, not a placeholder. Run one28 left thirteen
        // refusals recorded as `repeated_call` with an empty input, so an operator
        // reading the journal could not tell which call Runtime had stopped.
        let record = self
            .start_tool_invocation_record(tool_use_id, tool_name, input, 0)
            .refused(reason, now_ms());
        let refs = vec![
            RuntimeEventRef {
                kind: "tool_invocation".to_string(),
                id: record.invocation_id.clone(),
            },
            RuntimeEventRef {
                kind: "tool_call".to_string(),
                id: record.tool_call_id.clone(),
            },
        ];
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            "tool.invocation.refused",
            Some(record.status.as_str().to_string()),
            refs,
            serde_json::json!({
                "status": record.status.as_str().to_string(),
                "invocation_id": record.invocation_id,
                "tool_call_id": record.tool_call_id,
                "tool_name": record.tool_name,
                "turn_id": turn_id,
                "input_hash": record.input_hash,
                "cause_signature": "repeated_identical_call",
            }),
        );
        ConversationMessage::tool_result(
            tool_use_id.to_string(),
            record.tool_name.clone(),
            reason.to_string(),
            true,
        )
    }

    pub(crate) fn append_repetition_diagnosis(
        &self,
        tool_name: &str,
        input_hash: &str,
        input_refs: &[String],
        is_error: bool,
        failure_kind: ToolFailureKind,
        combined: &mut String,
    ) {
        // The breaker answers "the same dead end again", so only a failure belongs in
        // its ledger. The kind here was defaulted to `Unknown`, which
        // `counts_toward_breaker` accepts, so a completed call also incremented the
        // count. In one live run 40 of 62 refusals were issued against inputs that
        // had never failed - the model was told "this exact failure has now occurred
        // 2 times" about calls it had just succeeded at twice.
        if !is_error {
            return;
        }
        let turn_id = self.active_repetition_scope();
        if let Some(diagnosis) = crate::repetition_guard::record_tool_failure(
            &turn_id,
            tool_name,
            input_hash,
            input_refs,
            failure_kind,
            combined,
        ) {
            combined.push('\n');
            combined.push('\n');
            combined.push_str(&diagnosis);
        }
    }

    pub(super) fn tool_model_receipt(
        &self,
        tool_name: &str,
        output: &str,
        is_error: bool,
        raw_ref: &EvidenceRef,
        access: Option<&EvidenceAccessRef>,
        delivery_requirement: &ToolModelDeliveryRequirement,
        input: &str,
    ) -> crate::context_evidence::ModelReceipt {
        let raw_tokens = crate::context_ledger::estimate_text_tokens(output);
        let plan = if delivery_requirement.is_exact() && !is_error {
            Self::apply_exact_evidence_delivery_budget(self.runtime_budget_plan())
        } else {
            self.runtime_budget_plan()
        };
        let per_tool_limit = plan.tool_result_budget.per_tool_max_tokens as u64;
        if delivery_requirement.is_exact() && !is_error {
            if let Ok(mut ledger) = self.turn_context_ledger.lock() {
                ledger.expand_tool_result_limit(plan.tool_result_budget.max_total_tokens as u64);
            }
        }
        // `build_tool_receipt` spends part of the granted budget on its
        // evidence URI and structured summary prefix. Reserving only the raw
        // body size made even a tiny exact `read_file` JSON lose its `content`
        // field to head-tail truncation. Keep bounded headroom for the receipt
        // envelope while preserving the existing per-tool hard ceiling.
        // Reserve a serialization allowance (codex parity, x1.2) so the receipt
        // envelope and the omission marker are not themselves re-truncated when
        // the message is serialized at the model boundary.
        let requested = raw_tokens
            .saturating_add(raw_tokens / 5)
            .saturating_add(96)
            .min(per_tool_limit)
            .max(1);
        // PLAN B-5: a read must never be starved into a shell by a spent
        // tool-result budget. For the read family the budget is a soft cap, so an
        // exhausted ledger still grants this floor.
        let read_tool = matches!(
            tool_name,
            "read_file" | "read_ref" | "grep_search" | "glob_search" | "workspace_snapshot"
        );
        let granted = self
            .turn_context_ledger
            .lock()
            .map(|mut ledger| {
                if read_tool {
                    ledger.reserve_tool_result_with_floor(requested, READ_TOOL_RESULT_FLOOR_TOKENS)
                } else {
                    ledger.reserve_tool_result(requested)
                }
            })
            .unwrap_or(requested);
        let preserve_complete_read = tool_name == "read_file" && !is_error
            && (delivery_requirement.is_exact()
                || serde_json::from_str::<serde_json::Value>(input).ok()
                    .is_some_and(|value| value["complete"].as_bool() == Some(true)));
        let mut receipt = crate::context_evidence::build_tool_receipt_for_delivery(
            tool_name,
            output,
            is_error,
            raw_ref.clone(),
            granted.max(24),
            preserve_complete_read,
        );
        if access.is_none() {
            if receipt.summary.starts_with("Tool `") {
                receipt.summary = receipt.summary.replacen(
                    "Evidence: tool://",
                    "Ephemeral evidence (active runtime only): tool://",
                    1,
                );
            }
            receipt.receipt_tokens = crate::context_ledger::estimate_text_tokens(&receipt.summary);
        }
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::ToolResult,
            receipt.receipt_tokens,
            access.map(|_| format!("tool://{}", raw_ref.id())),
            self.session_head_blocking().message_count,
        );
        receipt
    }

    #[allow(dead_code)]
    pub(super) fn start_tool_invocation_record(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        iterations: usize,
    ) -> ToolInvocationRecord {
        let session_id = self.session_id().to_string();
        let safety_category = serde_json::from_str::<serde_json::Value>(input)
            .ok()
            .and_then(|input| self.tool_executor.registered_tool_effect(tool_name, &input))
            .map_or(crate::ToolSafetyCategory::Destructive, |effect| {
                crate::ToolSafetyCategory::from_effect(&effect)
            });
        ToolInvocationRecord::started(
            session_id,
            iterations,
            tool_use_id.to_string(),
            tool_name.to_string(),
            input,
            safety_category,
            now_ms(),
        )
    }

    pub(super) fn record_tool_invocation_denied(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        iterations: usize,
        failure_kind: ToolFailureKind,
        reason: &str,
    ) {
        let record = self
            .start_tool_invocation_record(tool_use_id, tool_name, input, iterations)
            .failed(failure_kind, reason, now_ms());
        self.record_tool_invocation_event(
            &record,
            "tool.invocation.denied",
            self.session_head_blocking().message_count,
            None,
        );
    }

    pub(super) fn record_tool_invocation_event(
        &self,
        record: &ToolInvocationRecord,
        kind: &'static str,
        _sequence: usize,
        raw_access: Option<&harness_contract::context::EvidenceAccessRef>,
    ) {
        let mut payload = serde_json::to_value(record).unwrap_or_else(
            |error| serde_json::json!({ "serialization_error": error.to_string() }),
        );
        if let Some(access) = raw_access {
            let reference = format!("tool://{}", access.evidence_ref.id());
            let digest = access
                .sha256
                .strip_prefix("sha256:")
                .unwrap_or(&access.sha256);
            if !access.is_durable()
                || access.evidence_ref.ref_type != "tool"
                || record.session_id != self.session_id()
                || record.full_output_ref.as_deref() != Some(reference.as_str())
                || !access
                    .evidence_ref
                    .id()
                    .starts_with(&format!("tool-raw-{}-", record.tool_call_id))
                || access.visibility_scope != format!("session:{}", self.session_id())
                || !access.retrieval_selector.starts_with("artifact://")
                || digest.len() != 64
                || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                tracing::warn!("Tool terminal raw identity failed its original writer binding");
                return;
            }
            payload["runtime_raw_content_v1"] = serde_json::json!({
                "version":1, "ref_id":reference, "tool_call_id":record.tool_call_id,
                "sha256":access.sha256, "byte_count":access.bytes,
                "visibility_scope":access.visibility_scope, "selector":access.retrieval_selector,
                "media_type":access.media_type,
            });
        }
        let owner_cut = (kind == "tool.invocation.completed"
            && matches!(
                record.tool_name.as_str(),
                "read_file" | "read_ref" | "evidence_retrieve"
            ))
        .then(|| {
            self.runtime_event_store.as_ref().and_then(|store| {
                store
                    .stream_revision(&format!("session:{}", self.session_id()))
                    .ok()
            })
        })
        .flatten();
        let mut refs = vec![
            RuntimeEventRef {
                kind: "tool_invocation".to_string(),
                id: record.invocation_id.clone(),
            },
            RuntimeEventRef {
                kind: "tool_call".to_string(),
                id: record.tool_call_id.clone(),
            },
        ];
        if let Some(plan_id) = &record.governed_plan_id {
            refs.push(RuntimeEventRef {
                kind: "governed_tool_plan".to_string(),
                id: plan_id.clone(),
            });
        }
        #[cfg(test)]
        self.apply_raw_origin_encoding_for_test(record, kind, &mut payload);
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            kind,
            Some(record.status.as_str().to_string()),
            refs,
            payload.clone(),
        );
        if let Some(after_sequence) = owner_cut {
            if let Err(error) = self.capture_native_root_owner(record, &payload, after_sequence) {
                tracing::warn!(%error, "Native Root owner receipt remains unknown");
            }
        }
    }

    pub(crate) fn record_governed_tool_plan(
        &self,
        plan: &GovernedToolPlan,
    ) -> Result<(), RuntimeError> {
        if let Some(store) = self.runtime_event_store.as_ref() {
            let mut refs = vec![RuntimeEventRef {
                kind: "governed_tool_plan".to_string(),
                id: plan.plan_id.clone(),
            }];
            if let Some(context) = self
                .cowd_bus()
                .and_then(crate::CowdEventBus::current_execution_context)
            {
                refs.extend([
                    RuntimeEventRef {
                        kind: "execution".into(),
                        id: context.execution_id,
                    },
                    RuntimeEventRef {
                        kind: "session".into(),
                        id: context.session_id,
                    },
                    RuntimeEventRef {
                        kind: "turn".into(),
                        id: context.turn_id,
                    },
                ]);
            }
            store.append(RuntimeEventInput {
                stream_id: format!("session:{}", self.session_id()),
                scope: RuntimeEventScope::Tool,
                kind: "tool.execution_plan.created".into(),
                status: Some("planned".into()),
                actor: Some("conversation_runtime".into()),
                refs,
                payload: serde_json::to_value(plan)
                    .map_err(|error| RuntimeError::new(error.to_string()))?,
            })
            .map_err(RuntimeError::new)?;
        }
        if let Ok(mut plans) = self.turn_governed_tool_plans.lock() {
            plans.push(plan.projection());
        }
        Ok(())
    }

    pub(super) fn record_tool_strategy_validation(
        &self,
        report: &GovernedToolPolicyValidationReport,
        _sequence: usize,
    ) {
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            "tool.strategy_validation.completed",
            Some(if report.allowed { "allowed" } else { "denied" }.to_string()),
            vec![RuntimeEventRef {
                kind: "strategy_lease".to_string(),
                id: report.lease_id.clone(),
            }],
            serde_json::to_value(report).unwrap_or_else(|_| {
                serde_json::json!({
                    "allowed": false,
                    "findings": ["strategy_validation_serialization_failed"],
                    "lease_id": report.lease_id,
                })
            }),
        );
    }

    pub(super) fn record_tool_schedule(
        &self,
        plan: &GovernedToolPlan,
        requests: &[crate::tool_dispatch::ToolRequest],
        _sequence: usize,
    ) {
        self.append_execution_runtime_event(
            RuntimeEventScope::Schedule,
            "tool.schedule.created",
            Some("planned".to_string()),
            requests
                .iter()
                .map(|request| RuntimeEventRef {
                    kind: "tool_call".to_string(),
                    id: request.tool_use_id.clone(),
                })
                .collect(),
            serde_json::json!({
                "plan_id": plan.plan_id,
                "plan_revision": plan.revision,
                "topology_hash": plan.topology_hash,
                "topological_order": plan.topological_order,
                "task_count": plan.task_count,
                "tool_count": requests.len(),
            }),
        );
    }

    pub(super) fn record_ai_kernel_trace_event(
        &self,
        trace: &RuntimeAiKernelTrace,
        sequence: usize,
    ) {
        if self.runtime_event_store.is_none() {
            return;
        }
        let payload = serde_json::json!({
            "strategy": {
                "pattern": trace.execution_decision.strategy.pattern.as_str(),
                "confidence": trace.execution_decision.strategy.confidence,
                "policy_version": trace.execution_decision.strategy.policy_version,
                "reasons": trace.execution_decision.strategy.reasons,
                "required_capabilities": trace.execution_decision.strategy.required_capabilities.iter().map(|item| format!("{item:?}")).collect::<Vec<_>>(),
                "complexity": format!("{:?}", trace.execution_decision.strategy.understanding.complexity),
                "risk": format!("{:?}", trace.execution_decision.strategy.understanding.risk),
                "modifiers": trace.execution_decision.strategy.modifiers.iter().map(|item| item.as_str()).collect::<Vec<_>>(),
            },
            "context": {
                "epoch_id": trace.context_epoch.epoch_id,
                "envelope_id": trace.context_envelope_id,
                "token_total": trace.context_epoch.token_total,
                "selected_count": trace.context_epoch.selected.len(),
                "omitted_count": trace.context_epoch.omitted.len(),
                "alignment": trace.context_alignment,
            },
            "verification": {
                "can_finalize": trace.verification_report.can_finalize,
                "verification_blocked": trace.verification_blocked,
                "severity": format!("{:?}", trace.verification_report.severity),
                "blocking_reasons": trace.verification_report.blocking_reasons,
                "claim_count": trace.verification_report.claim_count,
                "evidence_count": trace.verification_report.evidence_count,
                "unsupported_required_count": trace.verification_report.unsupported_required_claims.len(),
                "not_run_count": trace.verification_report.not_run_claims.len(),
                "matrix_missing_evidence": matrix_missing_evidence(trace),
            },
            "governed_tool_plans": trace.governed_tool_plans.iter().map(|plan| serde_json::json!({
                "id": plan.plan_id,
                "revision": plan.revision,
                "catalog_revision": plan.catalog_revision,
                "invocation_count": plan.invocations.len(),
                "dependency_count": plan.dependencies.len(),
            })).collect::<Vec<_>>(),
            "harness": {
                "receipt_id": trace.harness_receipt.id,
                "harness_id": trace.harness_receipt.harness_id,
                "agent_spec_id": trace.harness_receipt.agent_spec_id,
                "strategy_pattern": trace.harness_receipt.strategy_pattern,
                "context_epoch_id": trace.harness_receipt.context_epoch_id,
                "governed_tool_plan_ids": trace.harness_receipt.governed_tool_plan_ids,
                "verification_can_finalize": trace.harness_receipt.verification_can_finalize,
                "policy_receipts": trace.harness_receipt.policy_receipts,
                "output_summary": trace.harness_receipt.output_summary,
            },
            "policy_receipts": trace.policy_receipts.iter().map(|receipt| serde_json::json!({
                "id": receipt.id,
                "scope": format!("{:?}", receipt.scope),
                "decision": format!("{:?}", receipt.decision),
                "reasons": receipt.reasons,
                "evidence_refs": receipt.evidence_refs,
                "source_policy": receipt.source_policy,
                "created_at": receipt.created_at,
            })).collect::<Vec<_>>(),
            "behavior_policy": {
                "necessity": trace.behavior_policy.necessity,
                "reuse_opportunities": trace.behavior_policy.reuse_opportunities,
                "overengineering_risks": trace.behavior_policy.overengineering_risks,
                "safety_exceptions": trace.behavior_policy.safety_exceptions,
                "recommended_scope": format!("{:?}", trace.behavior_policy.recommended_scope),
                "enforcement": {
                    "allow_execution": trace.behavior_policy.enforcement.allow_execution,
                    "requires_scope_downgrade": trace.behavior_policy.enforcement.requires_scope_downgrade,
                    "requires_human_review": trace.behavior_policy.enforcement.requires_human_review,
                },
                "eval_checks": trace.behavior_policy.eval_checks,
            },
            "execution_graph_ref": trace.execution_decision.execution_graph_ref,
            "bench": {
                "passed": trace.bench_result.passed,
                "score": trace.bench_result.score,
                "case_id": trace.bench_result.case_id,
                "reasons": trace.bench_result.reasons,
            },
            "regression_gate": {
                "allowed": trace.regression_gate.allowed,
                "average_score": trace.regression_gate.average_score,
                "failed": trace.regression_gate.failed,
                "reasons": trace.regression_gate.reasons,
            },
            "growth": {
                "record_id": trace.learning_record.id,
                "event_id": trace.growth_event.id,
                "policy": trace.learning_record.policy,
                "has_blocker": trace.learning_record.has_blocker(),
                "signals": trace.learning_record.signals.iter().map(|signal| serde_json::json!({
                    "kind": format!("{:?}", signal.kind),
                    "severity": format!("{:?}", signal.severity),
                    "summary": signal.summary,
                })).collect::<Vec<_>>(),
                "next_strategy_hints": trace.learning_record.next_strategy_hints,
            },
            "growth_event_schema_version": 1,
            "growth_event": trace.growth_event,
            "strategy_experience": strategy_experience_projection(trace),
            "maintenance_candidates": growth_maintenance_candidates(trace),
            "matrix_evidence_signal": {
                "source": "ai_kernel_trace",
                "growth_event_id": trace.growth_event.id,
                "packet_contract": {
                    "problem_statement": "AI harness execution quality",
                    "trace_ref": format!("runtime:event:{sequence}"),
                    "strategy_pattern": trace.execution_decision.strategy.pattern.as_str(),
                    "verification_can_finalize": trace.verification_report.can_finalize,
                    "regression_allowed": trace.regression_gate.allowed,
                    "harness_receipt_id": trace.harness_receipt.id,
                },
                "evidence_refs": trace.growth_event.evidence_refs,
                "signals": trace.growth_event.matrix_signals,
                "missing_evidence": matrix_missing_evidence(trace),
            },
        });
        self.append_execution_runtime_event(
            RuntimeEventScope::Task,
            "runtime.harness_contract.trace",
            Some(if trace.verification_report.can_finalize {
                "completed".to_string()
            } else {
                "degraded".to_string()
            }),
            vec![RuntimeEventRef {
                kind: "harness_receipt".to_string(),
                id: trace.harness_receipt.id.clone(),
            }],
            payload,
        );
    }
}

/// Consecutive ignored refusals after which the next model step is text-only.
///
/// Pure so the contract is testable: unset means 2, `0` disables the
/// escalation, and a value that does not parse falls back to the default
/// rather than silently removing the stop. The first spelling of the
/// `consecutive_answer_stop_bound` contract made the documented off switch do
/// the opposite of what it said; this one is checked by a test.
fn ignored_refusal_stop_bound() -> Option<usize> {
    ignored_refusal_stop_bound_from(std::env::var("COWD_IGNORED_REFUSAL_LIMIT").ok().as_deref())
}

/// The bound as a pure function of the configured value, so the contract is
/// testable without mutating the process environment.
fn ignored_refusal_stop_bound_from(raw: Option<&str>) -> Option<usize> {
    match raw {
        None => Some(2),
        Some(value) => match value.trim().parse::<usize>() {
            Ok(0) => None,
            Ok(bound) => Some(bound),
            Err(_) => Some(2),
        },
    }
}

/// The calls the model emitted that the governed plan neither scheduled nor
/// refused - the ones that were swallowed.
///
/// Run mon6 (2026-10-13) emitted `objective_review` - the closure rung, the
/// first time any run reached for it - and the call vanished: no invocation,
/// no plan rejection, no early-defer, no exposure denial, no warning. The
/// provider attempt had completed; the node replanned the same second on a
/// completion gap. The model never learned its closing action had been eaten,
/// went back to reading, and the monitor stopped the run twelve minutes later.
///
/// Pure so the accountability invariant is testable without a Runtime: given
/// what was emitted, what the plan scheduled and what it refused, exactly the
/// swallowed set remains.
pub(crate) fn unaccounted_calls(
    pending: &[(String, String, String)],
    plan: &crate::governed_tool_plan::GovernedToolPlan,
    rejected: &[crate::governed_tool_plan::GovernedToolCompileRejection],
) -> Vec<(String, String)> {
    use std::collections::BTreeSet;
    let scheduled: BTreeSet<&str> = plan
        .tasks
        .iter()
        .map(|task| task.tool_call_id.as_str())
        .collect();
    let refused: BTreeSet<&str> = rejected
        .iter()
        .map(|entry| entry.tool_call_id.as_str())
        .collect();
    pending
        .iter()
        .filter(|(call_id, _, _)| {
            !scheduled.contains(call_id.as_str()) && !refused.contains(call_id.as_str())
        })
        .map(|(call_id, tool_name, _)| (call_id.clone(), tool_name.clone()))
        .collect()
}

#[cfg(test)]
mod agentic_action_body_tests {
    use super::agentic_action_body;

    fn assert_original_process_feedback_708(
        outcome: crate::RuntimeToolExecutionOutcome,
        expected_error: bool,
    ) {
        assert_eq!(outcome.model_feedback_is_error(), expected_error);
        let original_observations = outcome.observed_evidence.clone();
        let live = super::replay_tool_output(outcome.clone()).unwrap();
        let (live, is_error, failure) = super::model_tool_output_feedback("execute_code", live);
        assert_eq!(is_error, expected_error);
        assert_eq!(failure.is_some(), expected_error);
        assert_eq!(live.observations(), original_observations);
        let retained = crate::bound_tool_batch::retained_replay_outcome(outcome);
        assert_eq!(retained.model_feedback_is_error(), expected_error);
        let replay = super::replay_tool_output(retained.clone()).unwrap();
        let (replay, replay_error, _) = super::model_tool_output_feedback("execute_code", replay);
        assert_eq!(replay_error, expected_error);
        assert_eq!(replay.artifact_ref(), live.artifact_ref());
        assert_eq!(replay.model_text(), live.model_text());
        assert_eq!(replay.observations(), retained.observed_evidence);
        for (fresh, replayed) in original_observations.iter().zip(replay.observations()) {
            assert_eq!(fresh.target, replayed.target);
            assert_eq!(fresh.observed_at_sequence, replayed.observed_at_sequence);
            assert_eq!(
                replayed.provenance,
                harness_contract::context::ObservedEvidenceProvenance::RetainedReplay
            );
        }
    }

    #[tokio::test]
    async fn r1_708_actual_payload_feedback_preserves_original_receipt_and_exact_replay() {
        use crate::execution_core::graph::bounded_process_tests::{owned_fixture, request};
        use harness_contract::context::{EvidenceTargetIdentity, ToolExecutionTerminal};
        for (case, code, expected_exit, expected_signal, expected_error) in [
            ("zero", "print('ok')", Some(0), None, false),
            ("capture-boundary", "import sys; sys.stdout.write('x'*2097152); sys.stdout.flush()", Some(0), None, false),
            ("nonzero", "import sys; sys.exit(7)", Some(7), None, true),
            ("signal", "import os,signal,time\nif os.fork()==0:\n os.setsid()\n time.sleep(60)\n os._exit(0)\ntime.sleep(0.1)\nos.kill(os.getpid(),signal.SIGTERM)", None, Some(15), true),
        ] {
            let fixture = owned_fixture();
            let request = request(&fixture.host, serde_json::json!({
                "language":"python", "code":code, "input_files":[], "timeout_ms":5000
            }));
            let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
            let service = fixture.services.commit_service().clone();
            let authority = crate::tool_host::BoundedProcessAuthority::bind(fixture.services.clone(), request.clone()).unwrap();
            let outcome = crate::bound_tool_batch::execute_bounded_process_owned(
                fixture.host.pin_snapshot(), fixture.services.tool_execution_plane().clone(), authority,
                service.clone(), request.clone(), effect.clone(), Some(fixture.root.clone()), Some((fixture.owner.clone(), 1)),
            ).await.unwrap();
            let EvidenceTargetIdentity::ToolExecution { execution } = &outcome.observed_evidence[0].target else { panic!("{case}: missing original fact") };
            assert!(matches!(&execution.terminal, ToolExecutionTerminal::Settled { exit_code, signal, closure, .. }
                if *exit_code == expected_exit && *signal == expected_signal && closure.all_children_reaped && closure.init_reaped), "{case}: {:?}", execution.terminal);
            assert_eq!(outcome.status, crate::RuntimeToolExecutionStatus::Executed);
            assert!(crate::execution_projection::settlement::process_closed(&outcome, &effect));
            assert_original_process_feedback_708(outcome.clone(), expected_error);
            let stored = service.completed_tool_effect(&request, &effect).unwrap().unwrap();
            assert_eq!(stored, outcome);
            let authority = crate::tool_host::BoundedProcessAuthority::bind(fixture.services.clone(), request.clone()).unwrap();
            let replay = crate::bound_tool_batch::execute_bounded_process_owned(
                fixture.host.pin_snapshot(), fixture.services.tool_execution_plane().clone(), authority,
                service, request, effect, None, None,
            ).await.unwrap();
            assert_eq!(replay, crate::bound_tool_batch::retained_replay_outcome(outcome));
            assert_original_process_feedback_708(replay, expected_error);
            assert_eq!(fixture.services.tool_execution_plane().stats().submitted, 1);
        }
    }

    #[tokio::test]
    async fn r1_708_actual_timeout_and_capture_limit_are_payload_errors_not_unknown() {
        use crate::execution_core::graph::bounded_process_tests::{owned_fixture, request};
        use harness_contract::context::{
            EvidenceTargetIdentity, ToolExecutionStopReason, ToolExecutionTerminal,
        };
        for (code, timeout, stop) in [
            (
                "import time; time.sleep(30)",
                2500,
                ToolExecutionStopReason::Timeout,
            ),
            (
                "import sys; sys.stdout.write('x'*2097153); sys.stdout.flush()",
                5000,
                ToolExecutionStopReason::CaptureLimit,
            ),
        ] {
            let fixture = owned_fixture();
            let request = request(
                &fixture.host,
                serde_json::json!({"language":"python","code":code,"input_files":[],"timeout_ms":timeout}),
            );
            let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
            let authority = crate::tool_host::BoundedProcessAuthority::bind(
                fixture.services.clone(),
                request.clone(),
            )
            .unwrap();
            let outcome = crate::bound_tool_batch::execute_bounded_process_owned(
                fixture.host.pin_snapshot(),
                fixture.services.tool_execution_plane().clone(),
                authority,
                fixture.services.commit_service().clone(),
                request.clone(),
                effect.clone(),
                Some(fixture.root.clone()),
                Some((fixture.owner.clone(), 1)),
            )
            .await
            .unwrap();
            let EvidenceTargetIdentity::ToolExecution { execution } =
                &outcome.observed_evidence[0].target
            else {
                panic!("missing original fact")
            };
            assert!(
                matches!(&execution.terminal, ToolExecutionTerminal::Settled { stop_reason, .. } if *stop_reason == stop),
                "{:?}",
                execution.terminal
            );
            assert!(crate::execution_projection::settlement::process_closed(
                &outcome, &effect
            ));
            assert_original_process_feedback_708(outcome.clone(), true);
            assert_eq!(
                fixture
                    .services
                    .commit_service()
                    .completed_tool_effect(&request, &effect)
                    .unwrap()
                    .unwrap(),
                outcome
            );
        }
    }

    #[tokio::test]
    async fn r1_708_actual_prelaunch_cancellation_is_error_without_forged_start() {
        use crate::execution_core::graph::bounded_process_tests::{
            marker_processes, owned_fixture, request,
        };
        use harness_contract::context::{
            ArtifactWriteDescriptor, EvidenceTargetIdentity, ToolExecutionStopReason,
            ToolExecutionTerminal,
        };
        for started in [false, true] {
            let fixture = owned_fixture();
            let marker = format!("cowd-feedback-cancel-708-{}", uuid::Uuid::new_v4());
            let code = if started {
                format!("import os\nos.execl('/bin/sleep','{marker}','30')")
            } else {
                "print('must not run')".into()
            };
            let input = serde_json::json!({"language":"python","code":code,"input_files":[],"timeout_ms":5000});
            let request = request(&fixture.host, input.clone());
            let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
            let service = fixture.services.commit_service();
            let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(
                fixture.host.pin_snapshot().workspace_root(),
            )
            .unwrap();
            let scope = crate::bound_tool_batch::admitted_effect_scope(
                &request,
                &effect,
                Some(fixture.host.pin_snapshot()),
                &resolver,
            )
            .unwrap();
            service
                .begin_root_tool_effect(&request, &effect, &fixture.root, &scope, &fixture.owner, 1)
                .unwrap();
            let prepared = fixture
                .host
                .pin_snapshot()
                .prepare_bounded_process(
                    request.authorization.as_ref().unwrap(),
                    &input,
                    service.bounded_process_binding(&request, &effect).unwrap(),
                )
                .unwrap();
            let artifacts = fixture.services.artifact_store();
            let source = artifacts
                .write_bytes(
                    ArtifactWriteDescriptor {
                        media_type: "application/octet-stream".into(),
                        visibility_scope: "session:scope2-session".into(),
                        expected_bytes: Some(prepared.source_bundle_bytes().len() as u64),
                        original_name: None,
                    },
                    prepared.source_bundle_bytes(),
                )
                .await
                .unwrap();
            let admission = service
                .admit_bounded_process(&request, &effect, &prepared, &source)
                .unwrap();
            let cancellation = tools::ToolExecutionCancellation::new();
            let completed = if started {
                let signal = cancellation.clone();
                let worker = tokio::task::spawn_blocking(move || prepared.run(admission, &signal));
                tokio::time::timeout(std::time::Duration::from_secs(4), async {
                    while marker_processes(&marker).is_empty() {
                        assert!(
                            !worker.is_finished(),
                            "must witness actual start before cancellation"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap();
                cancellation.cancel();
                let completed = tokio::time::timeout(std::time::Duration::from_secs(10), worker)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    marker_processes(&marker).is_empty(),
                    "actual payload must disappear before model feedback"
                );
                assert!(matches!(
                    completed.terminal(),
                    ToolExecutionTerminal::Settled {
                        stop_reason: ToolExecutionStopReason::Cancelled,
                        ..
                    }
                ));
                completed
            } else {
                cancellation.cancel();
                let completed = prepared.run(admission, &cancellation);
                assert!(
                    matches!(completed.terminal(), ToolExecutionTerminal::NotStarted { reason_code } if reason_code == "bounded_cancelled_before_spawn")
                );
                completed
            };
            let authority = crate::tool_host::BoundedProcessAuthority::bind(
                fixture.services.clone(),
                request.clone(),
            )
            .unwrap();
            let outcome = authority
                .with_commit_context(
                    service.commit_bounded_process(&request, &effect, completed, artifacts),
                )
                .await
                .unwrap();
            let EvidenceTargetIdentity::ToolExecution { execution } =
                &outcome.observed_evidence[0].target
            else {
                panic!("missing original fact")
            };
            assert_eq!(execution.stdout_bytes, 0);
            assert_eq!(execution.stderr_bytes, 0);
            assert_eq!(
                matches!(execution.terminal, ToolExecutionTerminal::NotStarted { .. }),
                !started
            );
            assert!(crate::execution_projection::settlement::process_closed(
                &outcome, &effect
            ));
            assert_original_process_feedback_708(outcome.clone(), true);
            assert_eq!(
                service
                    .completed_tool_effect(&request, &effect)
                    .unwrap()
                    .unwrap(),
                outcome
            );
        }
    }

    #[tokio::test]
    async fn r1_708_actual_control_loss_remains_unknown_and_blocks_physical_closure() {
        use crate::execution_core::graph::bounded_process_tests::{owned_fixture, request};
        use harness_contract::context::{EvidenceTargetIdentity, ToolExecutionTerminal};
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        let fixture = owned_fixture();
        let marker = format!("cowd-feedback-control-loss-708-{}", uuid::Uuid::new_v4());
        let request = request(
            &fixture.host,
            serde_json::json!({"language":"python", "input_files":[], "timeout_ms":5000,
            "code":format!("import os\nos.execl('/bin/sleep','{marker}','30')")}),
        );
        let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
        let service = fixture.services.commit_service().clone();
        let authority = crate::tool_host::BoundedProcessAuthority::bind(
            fixture.services.clone(),
            request.clone(),
        )
        .unwrap();
        let worker = {
            let plane = fixture.services.tool_execution_plane().clone();
            let lease = fixture.host.pin_snapshot();
            let root = fixture.root.clone();
            let owner = fixture.owner.clone();
            let service = service.clone();
            let request = request.clone();
            let effect = effect.clone();
            tokio::spawn(async move {
                crate::bound_tool_batch::execute_bounded_process_owned(
                    lease,
                    plane,
                    authority,
                    service,
                    request,
                    effect,
                    Some(root),
                    Some((owner, 1)),
                )
                .await
            })
        };
        let parent_pid = |process: u32| -> Option<u32> {
            std::fs::read_to_string(format!("/proc/{process}/status"))
                .ok()?
                .lines()
                .find_map(|line| line.strip_prefix("PPid:")?.trim().parse().ok())
        };
        tokio::time::timeout(std::time::Duration::from_secs(4), async {
            loop {
                for entry in std::fs::read_dir("/proc").unwrap().filter_map(Result::ok) {
                    let Ok(payload) = entry.file_name().to_string_lossy().parse::<u32>() else {
                        continue;
                    };
                    let Ok(command) = std::fs::read(entry.path().join("cmdline")) else {
                        continue;
                    };
                    if command.split(|byte| *byte == 0).next() != Some(marker.as_bytes()) {
                        continue;
                    }
                    let Some(init) = parent_pid(payload) else {
                        continue;
                    };
                    let init_command =
                        std::fs::read(format!("/proc/{init}/cmdline")).unwrap_or_default();
                    if !init_command
                        .split(|byte| *byte == 0)
                        .any(|argument| argument == b"--bounded-inner")
                    {
                        continue;
                    }
                    let namespace_pid1 = std::fs::read_to_string(format!("/proc/{init}/status"))
                        .unwrap_or_default()
                        .lines()
                        .any(|line| {
                            line.strip_prefix("NSpid:")
                                .is_some_and(|ids| ids.split_whitespace().last() == Some("1"))
                        });
                    assert!(namespace_pid1);
                    let mut ancestor = init;
                    for _ in 0..16 {
                        if ancestor == std::process::id() {
                            break;
                        }
                        ancestor = parent_pid(ancestor).unwrap_or(0);
                        if ancestor == 0 {
                            break;
                        }
                    }
                    assert_eq!(
                        ancestor,
                        std::process::id(),
                        "fault injection must own the original process tree"
                    );
                    let descriptor =
                        unsafe { libc::syscall(libc::SYS_pidfd_open, init as libc::pid_t, 0) };
                    assert!(descriptor >= 0);
                    let descriptor = unsafe { OwnedFd::from_raw_fd(descriptor as i32) };
                    assert_eq!(parent_pid(payload), Some(init));
                    assert_eq!(
                        std::fs::read(format!("/proc/{init}/cmdline")).unwrap(),
                        init_command
                    );
                    assert_eq!(
                        unsafe {
                            libc::syscall(
                                libc::SYS_pidfd_send_signal,
                                descriptor.as_raw_fd(),
                                libc::SIGKILL,
                                std::ptr::null::<libc::siginfo_t>(),
                                0,
                            )
                        },
                        0
                    );
                    return;
                }
                assert!(
                    !worker.is_finished(),
                    "original producer ended before controlled fault"
                );
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("must witness original payload before control loss");
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let EvidenceTargetIdentity::ToolExecution { execution } =
            &outcome.observed_evidence[0].target
        else {
            panic!("missing original fact")
        };
        assert!(matches!(
            &execution.terminal,
            ToolExecutionTerminal::Unknown { .. }
        ));
        assert!(!crate::execution_projection::settlement::process_closed(
            &outcome, &effect
        ));
        assert_eq!(outcome.status, crate::RuntimeToolExecutionStatus::Executed);
        assert_original_process_feedback_708(outcome.clone(), true);
        assert_eq!(
            service
                .completed_tool_effect(&request, &effect)
                .unwrap()
                .unwrap(),
            outcome
        );
    }

    #[test]
    fn retained_process_presentation_keeps_body_and_original_metadata_291() {
        use harness_contract::context::{
            ArtifactRef, EvidenceTargetIdentity, ObservedEvidence, ObservedEvidenceProvenance,
            ToolExecutionAdmission, ToolExecutionBinding, ToolExecutionFact, ToolExecutionTerminal,
        };
        let body = ArtifactRef::durable(
            "presentation-body",
            "sha256:test-body",
            27,
            "application/json",
            "session:presentation",
        );
        let observed = ObservedEvidence {
            obligation_id: String::new(),
            target: EvidenceTargetIdentity::ToolExecution {
                execution: ToolExecutionFact {
                    version: 1,
                    admission: ToolExecutionAdmission {
                        version: 1,
                        binding: ToolExecutionBinding {
                            call_key: "original-call".into(),
                            tool_use_id: "original-use".into(),
                            input_sha256: "1".repeat(64),
                            descriptor_sha256: "2".repeat(64),
                            authority_sha256: "3".repeat(64),
                        },
                        code_sha256: "4".repeat(64),
                        inputs: Vec::new(),
                        source_bundle: body.clone(),
                    },
                    terminal: ToolExecutionTerminal::NotStarted {
                        reason_code: "presentation_only".into(),
                    },
                    body: body.clone(),
                    stdout_sha256: "5".repeat(64),
                    stderr_sha256: "6".repeat(64),
                    stdout_bytes: 0,
                    stderr_bytes: 0,
                    capture_complete: true,
                },
            },
            observed_at_sequence: 291,
            tool_name: "execute_code".into(),
            provenance: ObservedEvidenceProvenance::RetainedReplay,
            evidence_ref: None,
            model_observation: None,
            workspace_prior_state: None,
        };
        let outcome = crate::RuntimeToolExecutionOutcome {
            tool_use_id: "original-use".into(),
            tool_name: "execute_code".into(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: crate::ToolSafetyCategory::ReadOnly,
            output: Some("bounded summary".into()),
            error: None,
            evidence_ref: "original-receipt".into(),
            observed_evidence: vec![observed.clone()],
        };
        let output = super::replay_tool_output(outcome.clone()).unwrap();
        assert_eq!(output.observations(), [observed]);
        assert_eq!(output.model_text(), "bounded summary");
        assert!(
            matches!(output,harness_contract::context::ToolOutputDraft::ObservedArtifact { artifact_ref,.. } if artifact_ref==body)
        );
        let mut failed = outcome;
        assert_original_process_feedback_708(failed.clone(), true);
        assert!(failed.model_feedback_is_error());
        let mut missing = failed.clone();
        missing.observed_evidence.clear();
        assert!(missing.model_feedback_is_error());
        missing.tool_name = "ordinary_tool".into();
        missing.output = Some(r#"{"exit_code":7,"is_error":true}"#.into());
        assert!(!missing.model_feedback_is_error());
        missing.status = crate::RuntimeToolExecutionStatus::BlockedPermission;
        assert!(missing.model_feedback_is_error());
        assert!(super::replay_tool_output(missing).is_err());
        let mut ambiguous = failed.clone();
        ambiguous
            .observed_evidence
            .extend(failed.observed_evidence.clone());
        assert!(ambiguous.model_feedback_is_error());
        failed.status = crate::RuntimeToolExecutionStatus::Failed;
        assert!(super::replay_tool_output(failed).is_err());
    }

    struct TicketApi;

    fn repetition_runtime(
        store: std::sync::Arc<crate::RuntimeEventStore>,
    ) -> super::ConversationRuntime<TicketApi, crate::StaticToolExecutor> {
        let mut session = super::Session::new();
        session.session_id = "session-dispatch".into();
        let runtime = super::ConversationRuntime::new(
            session,
            TicketApi,
            crate::StaticToolExecutor::new(),
            super::PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
            vec![],
        )
        .without_memory()
        .with_runtime_event_store(store);
        runtime.session_input_stream().set_active_turn(Some(
            harness_contract::turn::TurnId::from_string("turn-dispatch"),
        ));
        runtime
    }

    #[test]
    fn r1_repetition_same_session_turn_isolated_across_runtimes() {
        use crate::repetition_guard::*;

        let input = r#"{"path":"proof.txt"}"#;
        let input_hash = crate::tool_invocation::stable_hash(input);
        let first = repetition_runtime(std::sync::Arc::new(crate::RuntimeEventStore::for_test()));
        let first_scope = first.active_repetition_scope();
        for _ in 0..repeated_call_block_bound_for("read_file", input) {
            record_repeated_call(&first_scope, "read_file", &input_hash);
        }
        assert!(first.blocked_repeated_call("read_file", "first", input, 1).is_some());
        for _ in 0..8 {
            let fresh = repetition_runtime(std::sync::Arc::new(crate::RuntimeEventStore::for_test()));
            let fresh_scope = fresh.active_repetition_scope();
            assert_ne!(first_scope, fresh_scope);
            assert!(fresh.blocked_repeated_call("read_file", "fresh", input, 1).is_none());
            clear_failure_breakers(&fresh_scope);
            clear_breakers_for_role(&fresh_scope, true);
            assert!(first.blocked_repeated_call("read_file", "first", input, 1).is_some());
        }
        clear_turn(&first_scope);
    }

    #[test]
    fn r1_repetition_same_namespace_accumulates_across_delegated_graphs() {
        use crate::repetition_guard::*;

        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        let root = repetition_runtime(store.clone());
        let scope = root.active_repetition_scope();
        let input = r#"{"path":"proof.txt"}"#;
        let input_hash = crate::tool_invocation::stable_hash(input);
        let bound = repeated_call_block_bound_for("read_file", input);
        for graph_index in 0..bound {
            let bus = crate::CowdEventBus::new();
            let _execution = bus.enter_execution_with_activity(
                crate::CowdExecutionContext {
                    execution_id: format!("delegated-graph-{graph_index}"),
                    session_id: "session-dispatch".into(),
                    turn_id: "turn-dispatch".into(),
                },
                None,
            );
            let delegated = repetition_runtime(store.clone()).with_cowd_event_bus(bus);
            assert_eq!(scope, delegated.active_repetition_scope());
            assert!(delegated.blocked_repeated_call("read_file", "delegated", input, 1).is_none());
            record_repeated_call(&delegated.active_repetition_scope(), "read_file", &input_hash);
            assert!(!clear_breakers_for_role(&delegated.active_repetition_scope(), false));
            clear_failure_breakers(&delegated.active_repetition_scope());
        }
        let next_delegated = repetition_runtime(store);
        assert!(next_delegated.blocked_repeated_call("read_file", "next", input, 1).is_some());
        assert!(root.blocked_repeated_call("read_file", "root", input, 1).is_some());
        assert!(clear_breakers_for_role(&scope, true));
        assert!(root.blocked_repeated_call("read_file", "new-turn", input, 1).is_none());
        clear_turn(&scope);
    }

    impl super::ApiClient for TicketApi {
        fn stream(
            &mut self,
            _: super::ApiRequest,
        ) -> std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<super::AssistantEvent, super::RuntimeError>>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(futures::stream::empty())
        }
    }

    #[test]
    fn original_ticket_helper_hydrates_real_inline_and_artifact_graph_starts() {
        use crate::execution_core::graph::{
            ExecutionCommitService, ExecutionNodeBinding, NodeExecutionTicket,
        };
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionNodeStatus,
        };
        for artifact in [false, true] {
            for wave in [false, true] {
                let workspace = tempfile::tempdir().unwrap();
                let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
                store
                    .bind_artifact_store(std::sync::Arc::new(
                        crate::ArtifactStore::for_test_default(workspace.path().join("artifacts")),
                    ))
                    .unwrap();
                let service = ExecutionCommitService::new(store.clone());
                let session = super::Session::new();
                let mut graph = ExecutionGraph::new("original ticket ContentEnvelope");
                graph.lineage = Some(ExecutionGraphLineage {
                    session_id: session.session_id.clone(),
                    turn_id: "turn".into(),
                    root_task_id: "task".into(),
                    task_id: "task".into(),
                    generation: 1,
                });
                let mut node = ExecutionNodeSpec::new(
                    ExecutionNodeKind::ToolBatch,
                    "tool_batch",
                    "original-payload",
                );
                node.id = "original-node".into();
                graph.nodes.push(node);
                if wave && !artifact {
                    for index in 0..8 {
                        let mut sibling = ExecutionNodeSpec::new(
                            ExecutionNodeKind::ToolBatch,
                            "tool_batch",
                            "original topology padding".repeat(64),
                        );
                        sibling.id = format!("inactive-sibling-{index}");
                        graph.nodes.push(sibling);
                    }
                }
                graph = service.register_graph(graph).unwrap().graph;
                graph = service
                    .transition_node(
                        &graph,
                        "original-node",
                        ExecutionNodeStatus::Ready,
                        None,
                        vec![],
                    )
                    .unwrap()
                    .graph;
                let binding = ExecutionNodeBinding {
                    executor_kind: "tool_batch".into(),
                    ticket_idempotency_key: "original-attempt-key".into(),
                    attempt: 1,
                    resource_lease_refs: if artifact {
                        vec!["large-original-lease".repeat(2048)]
                    } else {
                        vec![]
                    },
                    scope_lease_ref: None,
                    worktree_lease_ref: None,
                };
                graph = if wave {
                    service
                        .bind_and_start_nodes(
                            &graph,
                            std::collections::BTreeMap::from([("original-node".into(), binding)]),
                            vec![],
                        )
                        .unwrap()
                        .graph
                } else {
                    service
                        .bind_and_start_node(&graph, "original-node", binding)
                        .unwrap()
                        .graph
                };
                let original = store
                    .latest_for_stream(&format!("{}:node:original-node", graph.id))
                    .unwrap()
                    .unwrap();
                let owner = original.activity_binding().unwrap();
                let root = crate::CowdExecutionContext {
                    execution_id: graph.id.clone(),
                    session_id: session.session_id.clone(),
                    turn_id: "turn".into(),
                };
                let bus = crate::CowdEventBus::new();
                let _scope = bus.enter_execution_with_activity(root, Some(owner.clone()));
                let runtime = super::ConversationRuntime::new(
                    session,
                    TicketApi,
                    crate::StaticToolExecutor::new(),
                    super::PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
                    vec![],
                )
                .without_memory()
                .with_runtime_event_store(store.clone())
                .with_cowd_event_bus(bus);
                let ticket = NodeExecutionTicket {
                    graph_id: graph.id.clone(),
                    node_id: "original-node".into(),
                    executor_kind: "tool_batch".into(),
                    service_class: graph.service_class,
                    attempt: 1,
                    idempotency_key: "original-attempt-key".into(),
                    payload_ref: "original-payload".into(),
                };
                assert_eq!(
                    runtime.root_effect_owner_for_ticket(&ticket).unwrap(),
                    (owner, 1)
                );
                let start = store
                    .list_stream_after(
                        &graph.id,
                        graph.revision - 1,
                        graph.revision,
                        1,
                        4 * 1024 * 1024,
                    )
                    .unwrap()
                    .remove(0);
                assert_eq!(start.schema_version, 2);
                assert_eq!(
                    start.payload["body"]["kind"],
                    if artifact { "artifact_ref" } else { "inline" }
                );
                if wave && !artifact {
                    assert_eq!(start.kind, "execution_graph.nodes_started");
                }
                if artifact {
                    assert_eq!(start.kind, "execution_graph.checkpoint");
                }
                assert!(super::original_ticket_start_event(&store, &start).is_ok());
                for field in ["attempt", "key", "payload", "node", "executor"] {
                    let mut foreign = ticket.clone();
                    match field {
                        "attempt" => foreign.attempt += 1,
                        "key" => foreign.idempotency_key = "foreign-key".into(),
                        "payload" => foreign.payload_ref = "foreign-payload".into(),
                        "node" => foreign.node_id = "foreign-node".into(),
                        "executor" => foreign.executor_kind = "foreign-executor".into(),
                        _ => unreachable!(),
                    }
                    assert!(
                        runtime.root_effect_owner_for_ticket(&foreign).is_err(),
                        "{field}"
                    );
                }
                for field in ["sha256", "bytes", "body"] {
                    let mut corrupt = start.clone();
                    corrupt.payload[field] = serde_json::Value::Null;
                    assert!(
                        super::original_ticket_start_event(&store, &corrupt).is_err(),
                        "{field}"
                    );
                }
            }
        }
    }

    #[test]
    fn root_effect_owner_uses_exact_activity_node_without_kind_or_parallel_inference() {
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionNodeStatus,
        };
        let root = crate::CowdExecutionContext {
            execution_id: "root".into(),
            session_id: "session".into(),
            turn_id: "turn".into(),
        };
        let owner: harness_contract::projection::RuntimeActivityBinding = serde_json::from_value(serde_json::json!({
            "root_execution_id":"root","session_id":"session","turn_id":"turn","root_task_id":"task","task_id":"task",
            "activity_id":"original-activity","node_id":"original-node","revision":3,"fence":3,"generation":2
        })).unwrap();
        for kind in [ExecutionNodeKind::ToolBatch, ExecutionNodeKind::InlineModel] {
            let mut graph = ExecutionGraph::new("root owner");
            graph.id = root.execution_id.clone();
            graph.revision = 3;
            graph.lineage = Some(ExecutionGraphLineage {
                session_id: "session".into(),
                turn_id: "turn".into(),
                root_task_id: "task".into(),
                task_id: "task".into(),
                generation: 2,
            });
            let mut original =
                ExecutionNodeSpec::new(kind, "original-executor", "original-payload");
            original.id = "original-node".into();
            let mut parallel = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "parallel-executor",
                "parallel-payload",
            );
            parallel.id = "parallel-node".into();
            graph.nodes = vec![original, parallel];
            for node in &graph.nodes {
                graph
                    .node_statuses
                    .insert(node.id.clone(), ExecutionNodeStatus::Running);
            }
            graph
                .recovery_cursor
                .node_attempts
                .insert("original-node".into(), 4);
            graph
                .recovery_cursor
                .node_attempts
                .insert("parallel-node".into(), 8);
            assert_eq!(
                super::authenticated_root_effect_owner(&root, owner.clone(), &graph).unwrap(),
                (owner.clone(), 4)
            );
            for mutation in [
                "missing-node",
                "foreign-node",
                "session",
                "turn",
                "graph",
                "agent",
                "generation",
                "task",
                "fence",
            ] {
                let mut foreign = owner.clone();
                match mutation {
                    "missing-node" => foreign.node_id = None,
                    "foreign-node" => foreign.node_id = Some("absent-node".into()),
                    "session" => foreign.session_id = "foreign-session".into(),
                    "turn" => foreign.turn_id = "foreign-turn".into(),
                    "graph" => foreign.root_execution_id = "foreign-root".into(),
                    "agent" => foreign.agent_run_id = Some("foreign-agent".into()),
                    "generation" => foreign.generation += 1,
                    "task" => foreign.task_id = "foreign-task".into(),
                    "fence" => foreign.fence = 0,
                    _ => unreachable!(),
                }
                assert!(
                    super::authenticated_root_effect_owner(&root, foreign, &graph).is_err(),
                    "{mutation}"
                );
            }
            graph
                .node_statuses
                .insert("original-node".into(), ExecutionNodeStatus::Completed);
            assert!(super::authenticated_root_effect_owner(&root, owner.clone(), &graph).is_err());
            graph
                .node_statuses
                .insert("original-node".into(), ExecutionNodeStatus::Running);
            graph.recovery_cursor.node_attempts.remove("original-node");
            assert!(super::authenticated_root_effect_owner(&root, owner.clone(), &graph).is_err());
        }
    }

    #[tokio::test]
    async fn root_canonical_wait_precedes_read_write_admission_and_keeps_original_commit_locked() {
        use super::*;
        use crate::execution_core::graph::{
            ExecutionCommitService, ExecutionNodeBinding, ScopeLockMode, ScopeLockRequest,
            ScopedResource,
        };
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionNodeStatus,
        };
        for read in [false, true] {
            let workspace = tempfile::tempdir().unwrap();
            std::fs::write(workspace.path().join("proof.txt"), "old body").unwrap();
            let host = tools::ToolHost::builtin("root-lock-repair", workspace.path());
            let request = crate::bound_tool_batch::scope_admission_tests::bound_request(
                &host,
                if read { "read_file" } else { "edit" },
                if read {
                    serde_json::json!({"path":"proof.txt"})
                } else {
                    serde_json::json!({"action":"write","path":"proof.txt","content":"written body"})
                },
            );
            let effect = &request.compiled_invocation.as_ref().unwrap().effect;
            let store = Arc::new(crate::RuntimeEventStore::for_test());
            let service = ExecutionCommitService::new(store.clone());
            let mut graph = ExecutionGraph::new("canonical root queue");
            graph.id = "scope2-graph".into();
            graph.lineage = Some(ExecutionGraphLineage {
                session_id: "scope2-session".into(),
                turn_id: "scope2-turn".into(),
                root_task_id: "task".into(),
                task_id: "task".into(),
                generation: 1,
            });
            let mut node = ExecutionNodeSpec::new(
                ExecutionNodeKind::ToolBatch,
                "tool_batch",
                "root-lock-repair",
            );
            node.id = "original-node".into();
            graph.nodes.push(node);
            graph = service.register_graph(graph).unwrap().graph;
            graph = service
                .transition_node(
                    &graph,
                    "original-node",
                    ExecutionNodeStatus::Ready,
                    None,
                    vec![],
                )
                .unwrap()
                .graph;
            graph = service
                .bind_and_start_node(
                    &graph,
                    "original-node",
                    ExecutionNodeBinding {
                        executor_kind: "tool_batch".into(),
                        ticket_idempotency_key: "root-lock-ticket".into(),
                        attempt: 1,
                        resource_lease_refs: vec![],
                        scope_lease_ref: None,
                        worktree_lease_ref: None,
                    },
                )
                .unwrap()
                .graph;
            let owner = store
                .latest_for_stream("scope2-graph:node:original-node")
                .unwrap()
                .unwrap()
                .activity_binding()
                .unwrap();
            let root = crate::CowdExecutionContext {
                execution_id: graph.id,
                session_id: "scope2-session".into(),
                turn_id: "scope2-turn".into(),
            };
            let mut session = Session::new();
            session.session_id = "scope2-session".into();
            let runtime = ConversationRuntime::new(
                session,
                TicketApi,
                crate::StaticToolExecutor::new(),
                PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
                vec![],
            )
            .without_memory()
            .with_runtime_event_store(store.clone());
            let plane = runtime.tool_execution_plane.clone();
            let locks = plane.scope_locks().clone();
            let resolver =
                crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                    .unwrap();
            let identity = resolver.resolve_planned_file("proof.txt").unwrap();
            let old_physical = resolver
                .physical_file_identity_at_admission(&identity)
                .unwrap();
            let writer_request = ScopeLockRequest {
                scope: ScopedResource::workspace_object(identity.clone()),
                mode: ScopeLockMode::Write,
            };
            let writer = locks
                .acquire(vec![writer_request.clone()], None)
                .await
                .unwrap();
            let demand = crate::governed_tool_plan::resource_demand_from_effect(effect);
            let effect_owner = (owner.clone(), 1);
            let cancelled = tokio::time::timeout(
                Duration::from_millis(10),
                plane.execute_async_classified_retained(
                    &demand,
                    None,
                    runtime.execution_service_class,
                    Some(runtime.execution_service_class),
                    Some("scope2-session"),
                    async {
                        runtime
                            .execute_scoped_root_effect(
                                &service,
                                &request,
                                effect,
                                Some(&root),
                                Some(&effect_owner),
                                false,
                                || async {
                                    Err(ToolError::new(
                                        "cancelled canonical waiter unexpectedly reached the tool",
                                    ))
                                },
                            )
                            .await
                    },
                ),
            )
            .await;
            assert!(cancelled.is_err());
            assert_eq!(
                store
                    .stream_revision(&format!("execution-effect:{}", request.idempotency_key))
                    .unwrap(),
                0
            );
            assert_eq!(locks.active_lease_count().unwrap(), 1);
            let entered = tokio::sync::Notify::new();
            let operation = async {
                entered.notify_one();
                runtime.execute_scoped_root_effect(&service, &request, effect, Some(&root), Some(&effect_owner), false, || async {
                    assert!(locks.acquire(vec![writer_request.clone()], Some(Duration::from_millis(5))).await.is_err());
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    assert!(locks.acquire(vec![writer_request.clone()], Some(Duration::from_millis(5))).await.is_err());
                    let intent_key = format!("{}:{}", request.idempotency_key, if read { "read-intent" } else { "intent" });
                    let intent = store.event_by_idempotency_key(&format!("execution-effect:{}", request.idempotency_key), &intent_key).unwrap().unwrap();
                    let scope: harness_contract::goal::ProofEffectScope = serde_json::from_value(intent.payload["resolved_effect_scope"].clone()).unwrap();
                    let target = scope.targets.iter().find(|target| target.path.workspace_relative_path == "proof.txt").unwrap();
                    assert_eq!(target.physical_identity, resolver.physical_file_identity_at_admission(&identity).unwrap());
                    assert_eq!(intent.payload["root_effect_owner"]["binding"], serde_json::to_value(&owner).unwrap());
                    let content = std::fs::read_to_string(workspace.path().join("proof.txt")).unwrap();
                    assert_eq!(content, "replacement body");
                    if !read { std::fs::write(workspace.path().join("proof.txt"), "written body").unwrap(); }
                    let output = if read { serde_json::json!({"type":"text","truncated":false,"file":{
                        "filePath":"proof.txt","content":content,"startLine":1,"numLines":1,"totalLines":1,
                        "sha256":crate::context_evidence::read_sha256(content.as_bytes()),"byteLength":content.len(),"endsWithNewline":false,
                    }}).to_string() } else { "written body".into() };
                    Ok(harness_contract::context::ToolOutputDraft::bounded_inline(output))
                }).await
            };
            let pending = plane.execute_async_classified_retained(
                &demand,
                Some(Duration::from_millis(100)),
                runtime.execution_service_class,
                Some(runtime.execution_service_class),
                Some("scope2-session"),
                operation,
            );
            tokio::pin!(pending);
            tokio::select! {
                result = &mut pending => panic!("canonical writer must block root admission: {:?}", result.0.is_ok()),
                () = entered.notified() => {}
            }
            let effect_stream = format!("execution-effect:{}", request.idempotency_key);
            assert_eq!(store.stream_revision(&effect_stream).unwrap(), 0);
            std::fs::write(workspace.path().join("replacement.txt"), "replacement body").unwrap();
            std::fs::rename(
                workspace.path().join("replacement.txt"),
                workspace.path().join("proof.txt"),
            )
            .unwrap();
            #[cfg(unix)]
            assert_ne!(
                old_physical,
                resolver
                    .physical_file_identity_at_admission(&identity)
                    .unwrap()
            );
            drop(writer);
            let (result, admission) = pending.await;
            result.unwrap().unwrap();
            assert_eq!(plane.stats().timed_out_waiters, 1);
            let suffix = if read { "read-receipt" } else { "receipt" };
            let primary = store
                .event_by_idempotency_key(
                    &effect_stream,
                    &format!("{}:{suffix}", request.idempotency_key),
                )
                .unwrap()
                .unwrap();
            let index = store
                .event_by_idempotency_key(
                    "execution-agent-receipts:scope2-graph:original-node:1",
                    &format!("agent-tool-receipt:{}", request.idempotency_key),
                )
                .unwrap()
                .unwrap();
            assert_eq!(primary.transaction_id, index.transaction_id);
            assert_eq!(
                primary.payload["resolved_effect_scope"],
                index.payload["resolved_effect_scope"]
            );
            let after = locks
                .acquire(
                    vec![writer_request.clone()],
                    Some(Duration::from_millis(20)),
                )
                .await
                .unwrap();
            drop(after);
            drop(admission);
        }
    }

    #[tokio::test]
    async fn root_physical_drift_rejects_original_scope_and_self_admission_does_not_reenter_child_locks(
    ) {
        use super::*;
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("proof.txt"), "old body").unwrap();
        let host = tools::ToolHost::builtin("root-drift", workspace.path());
        for read in [false, true] {
            let request = crate::bound_tool_batch::scope_admission_tests::bound_request(
                &host,
                if read { "read_file" } else { "edit" },
                if read {
                    serde_json::json!({"path":"proof.txt"})
                } else {
                    serde_json::json!({"action":"write","path":"proof.txt","content":"replacement"})
                },
            );
            let effect = &request.compiled_invocation.as_ref().unwrap().effect;
            let plane = ConversationRuntime::new(
                Session::new(),
                TicketApi,
                crate::StaticToolExecutor::new(),
                PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
                vec![],
            )
            .without_memory()
            .tool_execution_plane;
            let locked = lock_root_effect_scope(
                &request,
                effect,
                Some(host.pin_snapshot()),
                plane.scope_locks(),
                false,
            )
            .await
            .unwrap();
            assert!(validate_root_physical_scope(&locked.resolver, &locked.scope).is_ok());
            let original = locked.scope.clone();
            let delegated = tokio::time::timeout(
                Duration::from_millis(20),
                lock_root_effect_scope(
                    &request,
                    effect,
                    Some(host.pin_snapshot()),
                    plane.scope_locks(),
                    true,
                ),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(!delegated.scope.complete);
            assert!(delegated.scope.targets.is_empty());
            delegated.scope.validate_purpose_mapping(effect).unwrap();
            std::fs::write(
                workspace.path().join("external-replacement"),
                "external body",
            )
            .unwrap();
            std::fs::rename(
                workspace.path().join("external-replacement"),
                workspace.path().join("proof.txt"),
            )
            .unwrap();
            #[cfg(unix)]
            assert!(validate_root_physical_scope(&locked.resolver, &locked.scope).is_err());
            assert_eq!(locked.scope, original);
            drop(locked);
            #[cfg(unix)]
            for alias in ["hardlink", "symlink"] {
                let locked = lock_root_effect_scope(
                    &request,
                    effect,
                    Some(host.pin_snapshot()),
                    plane.scope_locks(),
                    false,
                )
                .await
                .unwrap();
                let original = locked.scope.clone();
                if alias == "hardlink" {
                    std::fs::hard_link(
                        workspace.path().join("proof.txt"),
                        workspace.path().join("alias.txt"),
                    )
                    .unwrap();
                } else {
                    std::fs::rename(
                        workspace.path().join("proof.txt"),
                        workspace.path().join("alias.txt"),
                    )
                    .unwrap();
                    std::os::unix::fs::symlink("alias.txt", workspace.path().join("proof.txt"))
                        .unwrap();
                }
                assert!(
                    validate_root_physical_scope(&locked.resolver, &locked.scope).is_err(),
                    "{alias}"
                );
                assert_eq!(locked.scope, original);
                if alias == "hardlink" {
                    std::fs::remove_file(workspace.path().join("alias.txt")).unwrap();
                } else {
                    std::fs::remove_file(workspace.path().join("proof.txt")).unwrap();
                    std::fs::rename(
                        workspace.path().join("alias.txt"),
                        workspace.path().join("proof.txt"),
                    )
                    .unwrap();
                }
                drop(locked);
            }
        }
    }

    #[tokio::test]
    async fn root_full_descriptor_locks_coordinate_checkpoint_process_and_independent_workspaces() {
        use super::*;
        use crate::execution_core::graph::{ScopeLockMode, ScopeLockRequest, ScopedResource};
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        for workspace in [first.path(), second.path()] {
            std::fs::write(workspace.join("proof.txt"), "original body").unwrap();
        }
        let first_host = tools::ToolHost::builtin("first-root-lock", first.path());
        let second_host = tools::ToolHost::builtin("second-root-lock", second.path());
        let request = crate::bound_tool_batch::scope_admission_tests::bound_request(
            &first_host,
            "tool_orchestrate",
            serde_json::json!({"steps":[
                {"id":"edit","op":"call","tool":"edit","input":{"action":"write","path":"proof.txt","content":"written body"}},
                {"id":"compute","op":"compute","language":"python","code":"print(1)"}
            ]}),
        );
        let effect = &request.compiled_invocation.as_ref().unwrap().effect;
        assert!(effect.spawns_process);
        let runtime = ConversationRuntime::new(
            Session::new(),
            TicketApi,
            crate::StaticToolExecutor::new(),
            PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
            vec![],
        )
        .without_memory();
        let locks = runtime.tool_execution_plane.scope_locks();
        let locked = lock_root_effect_scope(
            &request,
            effect,
            Some(first_host.pin_snapshot()),
            locks,
            false,
        )
        .await
        .unwrap();
        for path in [
            "proof.txt",
            ".cowd/checkpoints",
            "unrelated-process-target.txt",
        ] {
            let conflict = ScopeLockRequest {
                scope: ScopedResource::workspace_object(
                    locked.resolver.resolve_planned_file(path).unwrap(),
                ),
                mode: ScopeLockMode::Write,
            };
            assert!(
                locks
                    .acquire(vec![conflict], Some(Duration::from_millis(10)))
                    .await
                    .is_err(),
                "{path}"
            );
        }
        let independent = crate::bound_tool_batch::scope_admission_tests::bound_request(
            &second_host,
            "edit",
            serde_json::json!({"action":"write","path":"proof.txt","content":"independent body"}),
        );
        let independent_effect = &independent.compiled_invocation.as_ref().unwrap().effect;
        let independent_scope = tokio::time::timeout(
            Duration::from_millis(100),
            lock_root_effect_scope(
                &independent,
                independent_effect,
                Some(second_host.pin_snapshot()),
                locks,
                false,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(
            locked.resolver.workspace_id(),
            independent_scope.resolver.workspace_id()
        );
        assert_eq!(
            std::fs::read_to_string(first.path().join("proof.txt")).unwrap(),
            "original body"
        );
        drop(independent_scope);
        drop(locked);
    }

    #[test]
    fn a_call_the_plan_neither_scheduled_nor_refused_is_the_swallowed_one() {
        use crate::governed_tool_plan::ValidatedGovernedToolDag;
        use crate::tool_dispatch::ToolRequest;
        let requests = vec![
            ToolRequest {
                tool_use_id: "call-1".to_string(),
                tool_name: "read_file".to_string(),
                input: "{\"path\":\"a.rs\"}".to_string(),
                depends_on: Vec::new(),
            },
            ToolRequest {
                tool_use_id: "call-2".to_string(),
                tool_name: "read_file".to_string(),
                input: "{\"path\":\"b.rs\"}".to_string(),
                depends_on: Vec::new(),
            },
        ];
        let plan = ValidatedGovernedToolDag::from_requests(&requests);
        // mon6's shape: the model emitted a third call - the closure rung - and
        // the plan holds only the two reads. The third is the swallowed one.
        let pending = vec![
            (
                "call-1".to_string(),
                "read_file".to_string(),
                "{\"path\":\"a.rs\"}".to_string(),
            ),
            (
                "call-2".to_string(),
                "read_file".to_string(),
                "{\"path\":\"b.rs\"}".to_string(),
            ),
            (
                "call-3".to_string(),
                "objective_review".to_string(),
                "{\"criterion_ref\":\"user_intent\"}".to_string(),
            ),
        ];
        assert_eq!(
            super::unaccounted_calls(&pending, &plan, &[]),
            vec![("call-3".to_string(), "objective_review".to_string())]
        );
        // A refused call is accounted for, not swallowed.
        let rejected = vec![crate::governed_tool_plan::GovernedToolCompileRejection {
            tool_call_id: "call-3".to_string(),
            tool_name: "objective_review".to_string(),
            reason: "criterion unsatisfied".to_string(),
        }];
        assert!(super::unaccounted_calls(&pending, &plan, &rejected).is_empty());
        // Everything scheduled: nothing to report.
        assert!(super::unaccounted_calls(&pending[..2], &plan, &[]).is_empty());
    }

    #[test]
    fn the_ignored_refusal_bound_honors_its_documented_contract() {
        // Unset means the default of two ignored refusals.
        assert_eq!(super::ignored_refusal_stop_bound_from(None), Some(2));
        // A configured bound is honored as written.
        assert_eq!(super::ignored_refusal_stop_bound_from(Some("4")), Some(4));
        // The documented `0 disables` must disable, not mean "escalate on the
        // first refusal".
        assert_eq!(super::ignored_refusal_stop_bound_from(Some("0")), None);
        // A value that does not parse falls back to the default.
        assert_eq!(super::ignored_refusal_stop_bound_from(Some("")), Some(2));
        assert_eq!(super::ignored_refusal_stop_bound_from(Some("abc")), Some(2));
    }

    fn receipt(status: &str, changed_refs: &[&str], duplicate: bool) -> String {
        serde_json::json!({
            "receipt_id": "receipt:program:test:action",
            "action_id": "action",
            "action": "task_claim",
            "program_id": "program:test",
            "revision": 4,
            "status": status,
            "duplicate": duplicate,
            "changed_refs": changed_refs,
            "actionable": ["submit the deliverable"],
            "projection": null,
            "error": null
        })
        .to_string()
    }

    #[test]
    fn an_applied_agentic_receipt_is_delivered_verbatim() {
        let body = receipt("applied", &["task:t1"], false);
        assert_eq!(agentic_action_body(&body).as_deref(), Some(body.as_str()));
    }

    /// The half that must not change: a receipt that carries no new entity is not
    /// progress, and must keep the generic prose receipt rather than being
    /// delivered verbatim. This is what keeps "reading is not progress" intact.
    #[test]
    fn a_receipt_without_a_changed_entity_keeps_the_prose_receipt() {
        assert!(agentic_action_body(&receipt("applied", &[], false)).is_none());
        assert!(agentic_action_body(&receipt("applied", &["task:t1"], true)).is_none());
        assert!(agentic_action_body(&receipt("rejected", &["task:t1"], false)).is_none());
        assert!(agentic_action_body(&receipt("observed", &[], false)).is_none());
    }

    #[test]
    fn a_tool_that_is_not_an_agentic_receipt_keeps_the_prose_receipt() {
        assert!(agentic_action_body("{\"path\":\"a.rs\",\"content\":\"x\"}").is_none());
        assert!(agentic_action_body("plain text").is_none());
        assert!(agentic_action_body("").is_none());
    }
}

#[cfg(test)]
mod delegated_effect_parent_tests {
    use super::*;

    fn parent() -> (
        harness_contract::execution_graph::ExecutionParentBinding,
        u32,
    ) {
        (
            harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "agent-graph".into(),
                node_id: "agent-node".into(),
            },
            7,
        )
    }

    fn identity() -> harness_contract::execution::ExecutionIdentity {
        let graph = harness_contract::execution::ExecutionIdentity::for_task_graph(
            "principal",
            "workspace",
            "mission",
            "task",
            "session",
            "turn",
            "agent-graph",
        )
        .unwrap();
        harness_contract::execution::ExecutionIdentity::for_agent_node(
            &graph,
            "native-run-independent",
            "agent-node",
        )
        .unwrap()
    }

    fn activity() -> harness_contract::projection::RuntimeActivityBinding {
        serde_json::from_value(serde_json::json!({
            "root_execution_id":"original-root", "session_id":"session", "turn_id":"turn",
            "root_task_id":"root-task", "task_id":"task", "activity_id":"agent-activity",
            "node_id":"agent-node", "agent_run_id":"native-run-independent",
            "revision":3, "fence":2, "generation":4,
        }))
        .unwrap()
    }

    #[test]
    fn delegated_parent_keeps_packet_graph_node_and_noninitial_attempt() {
        let owner = activity();
        let checked = checked_delegated_effect_parent(
            Some(parent()),
            Some("native-run-independent"),
            Some(&identity()),
            Some(&owner),
            "session",
        )
        .unwrap();
        assert_eq!(checked, Some(parent()));
        assert!(checked_delegated_effect_parent(
            Some(parent()),
            Some("native-run-independent"),
            Some(&identity()),
            None,
            "session"
        )
        .is_ok());
        assert!(checked_delegated_effect_parent(
            Some(parent()),
            Some("native-run-independent"),
            None,
            Some(&owner),
            "session"
        )
        .is_err());
        assert!(
            checked_delegated_effect_parent(None, None, Some(&identity()), None, "session")
                .is_err()
        );
        assert!(
            checked_delegated_effect_parent(None, None, None, None, "session")
                .unwrap()
                .is_none()
        );
        let mut root = owner.clone();
        root.agent_run_id = None;
        assert!(
            checked_delegated_effect_parent(None, None, None, Some(&root), "session")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn delegated_parent_cannot_be_lost_or_borrow_root_or_other_agent() {
        let owner = activity();
        assert!(checked_delegated_effect_parent(
            None,
            Some("native-run-independent"),
            Some(&identity()),
            Some(&owner),
            "session"
        )
        .is_err());
        assert!(checked_delegated_effect_parent(
            None,
            None,
            Some(&identity()),
            Some(&owner),
            "session"
        )
        .is_err());
        for defect in [
            "graph",
            "node",
            "attempt",
            "step",
            "session",
            "root",
            "other-agent",
        ] {
            let (mut binding, mut attempt) = parent();
            let mut activity = owner.clone();
            let mut run = Some("native-run-independent");
            match defect {
                "graph" => binding.execution_id = "other-graph".into(),
                "node" => binding.node_id = "other-node".into(),
                "attempt" => attempt = 0,
                "step" => run = None,
                "session" => activity.session_id = "other-session".into(),
                "root" => activity.agent_run_id = None,
                "other-agent" => activity.agent_run_id = Some("run:other-agent".into()),
                _ => unreachable!(),
            }
            assert!(
                checked_delegated_effect_parent(
                    Some((binding, attempt)),
                    run,
                    Some(&identity()),
                    Some(&activity),
                    "session"
                )
                .is_err(),
                "{defect}"
            );
        }
    }
}

#[cfg(test)]
mod checkpoint_scope_366_tests {
    use super::*;
    use harness_contract::policy::{CapabilityGapKind, DataClassification, PermissionMode};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn plan(
        host: &tools::ToolHost,
        calls: &[(&str, &str, serde_json::Value)],
    ) -> crate::GovernedToolPlan {
        let lease = host.pin_snapshot();
        let requests = calls
            .iter()
            .map(|(id, name, input)| crate::tool_dispatch::ToolRequest {
                tool_use_id: (*id).into(),
                tool_name: (*name).into(),
                input: input.to_string(),
                depends_on: vec![],
            })
            .collect::<Vec<_>>();
        let mut plan = crate::GovernedToolCompiler
            .compile(lease.workspace_root(), &requests, |name, input| {
                Some((
                    lease.describe_effect(name, input),
                    lease.revision(),
                    lease.snapshot().descriptor_set_hash.clone(),
                ))
            })
            .unwrap();
        let step = Arc::new(
            crate::step_snapshot::BoundStep::bind(
                harness_contract::step::StepIdentity {
                    step_id: "r366-step".into(),
                    session_id: "r366-session".into(),
                    turn_id: "r366-turn".into(),
                    execution_id: Some("r366-graph".into()),
                    actor_ref: None,
                    run_ref: None,
                    attempt: None,
                    epoch: None,
                    goal_ref: None,
                    program_ref: None,
                    task_ref: None,
                    build_ref: "unit".into(),
                },
                lease,
                &[],
                &serde_json::json!({}),
                &serde_json::json!({}),
                BTreeMap::new(),
                i64::MAX,
            )
            .unwrap(),
        );
        for task in &mut plan.tasks {
            task.step_binding = Some(crate::step_snapshot::StepCallBinding::provider(
                step.clone(),
                &task.tool_call_id,
            ));
        }
        plan
    }

    fn assessment(
        lease: &tools::ToolHostLease,
        input: &serde_json::Value,
        ceiling: PermissionMode,
    ) -> crate::EffectiveToolAuthorizationAssessment {
        let effect = lease.describe_effect("checkpoint", input);
        let request = crate::AuthorizationRequest {
            principal_id: "agent:r366".into(),
            capability: "checkpoint".into(),
            input: input.to_string(),
            idempotency_key: "r366-cp".into(),
            effect,
            parent_ceiling: ceiling,
            parent_lease_id: Some("binding:r366".into()),
            policy_revision: 1,
            recovery_scope: "turn:r366-turn".into(),
            context: crate::PermissionContext::default(),
            safe_alternatives: vec![],
        };
        crate::AuthorizationNegotiator::new()
            .assess_effective(&crate::PermissionPolicy::new(ceiling), &request)
    }

    #[test]
    fn prepared_paths_authorize_and_snapshot_only_original_unreplayed_files() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("dir")).unwrap();
        for (path, bytes) in [
            ("a.txt", "original"),
            ("dir/b.txt", "directory body"),
            ("unrelated.txt", "foreign"),
            (".env", "must not be snapshotted"),
        ] {
            std::fs::write(root.path().join(path), bytes).unwrap();
        }
        let host = tools::ToolHost::builtin("r366", root.path());
        let lease = host.pin_snapshot();
        let old = serde_json::json!({"action":"create","label":"original auto checkpoint"});
        let denied = assessment(&lease, &old, PermissionMode::WorkspaceWrite);
        assert_eq!(
            denied.effective.descriptor.assessment.data_sensitivity,
            DataClassification::Secret
        );
        assert_eq!(
            denied.effective.descriptor.required_permission,
            PermissionMode::DangerFullAccess
        );
        assert_eq!(
            denied.assessment.gap.unwrap().kind,
            CapabilityGapKind::PermissionCeiling
        );
        let mut prepared = plan(
            &host,
            &[
                (
                    "replay",
                    "edit",
                    serde_json::json!({"action":"write","path":"unrelated.txt","content":"never replay mutation"}),
                ),
                (
                    "fresh",
                    "edit",
                    serde_json::json!({"action":"write","path":"a.txt","content":"after"}),
                ),
                (
                    "directory",
                    "edit",
                    serde_json::json!({"action":"write","path":"dir","content":"not executed by this scope unit"}),
                ),
                (
                    "missing",
                    "edit",
                    serde_json::json!({"action":"write","path":"new/file.txt","content":"new"}),
                ),
            ],
        );
        // The actual caller supplies admission_plan after this existing replay
        // exclusion, never the original plan that still owns retained results.
        prepared.tasks.retain(|task| task.tool_call_id != "replay");
        let paths = checkpoint_paths_for_admission(&prepared, Some(lease.clone())).unwrap();
        assert_eq!(paths, vec!["a.txt", "dir", "new/file.txt"]);
        let input = serde_json::json!({"action":"create","paths":paths});
        let assessed = assessment(&lease, &input, PermissionMode::WorkspaceWrite);
        assert_eq!(
            assessed.effective.descriptor.required_permission,
            PermissionMode::WorkspaceWrite
        );
        assert_ne!(
            assessed.assessment.gap.as_ref().map(|g| g.kind),
            Some(CapabilityGapKind::PermissionCeiling)
        );
        let cp = tools::checkpoint::checkpoint_create_in(
            root.path(),
            serde_json::from_value(input).unwrap(),
        )
        .unwrap();
        let saved = std::path::PathBuf::from(cp.path);
        assert_eq!(std::fs::read(saved.join("a.txt")).unwrap(), b"original");
        assert_eq!(
            std::fs::read(saved.join("dir/b.txt")).unwrap(),
            b"directory body"
        );
        assert!(
            !saved.join("unrelated.txt").exists()
                && !saved.join(".env").exists()
                && !saved.join("new/file.txt").exists()
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &std::fs::read(saved.join(".manifest.json")).unwrap()
            )
            .unwrap()["scopes"],
            serde_json::json!(["a.txt", "dir", "new/file.txt"])
        );
        assert_eq!(
            assessment(
                &lease,
                &serde_json::json!({"action":"create","paths":["a.txt"]}),
                PermissionMode::ReadOnly
            )
            .assessment
            .gap
            .unwrap()
            .kind,
            CapabilityGapKind::PermissionCeiling
        );
        let mut unknown = prepared.clone();
        unknown.tasks[0].invocation.effect.effect_kind =
            harness_contract::tool::ToolEffectKind::Unknown;
        assert!(checkpoint_paths_for_admission(&unknown, Some(lease.clone())).is_err());
        let mut empty = prepared.clone();
        empty.tasks.clear();
        assert!(checkpoint_paths_for_admission(&empty, Some(lease.clone())).is_err());
        let nonfile = plan(
            &host,
            &[("process", "bash", serde_json::json!({"command":"true"}))],
        );
        let mut mixed = prepared.clone();
        mixed.tasks.extend(nonfile.tasks);
        assert!(checkpoint_paths_for_admission(&mixed, Some(lease.clone())).is_err());
        for path in ["../outside", ".", ".cowd/secret"] {
            // The original compiler may reject first; either way this is not
            // silently dropped from a plan and replaced by a broad parent.
            let mut invalid = prepared.clone();
            invalid.tasks[0].normalized_input["path"] = serde_json::json!(path);
            assert!(checkpoint_paths_for_admission(&invalid, Some(lease.clone())).is_err());
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("a.txt", root.path().join("alias")).unwrap();
            let link = plan(
                &host,
                &[(
                    "link",
                    "edit",
                    serde_json::json!({"action":"write","path":"alias","content":"after"}),
                )],
            );
            let paths = checkpoint_paths_for_admission(&link, Some(lease)).unwrap();
            assert_eq!(paths, vec!["alias"]);
            assert!(
                tools::checkpoint::checkpoint_create_in(
                    root.path(),
                    tools::checkpoint::CheckpointCreateInput { label: None, paths }
                )
                .is_err()
            );
        }
    }

    struct Api;
    impl ApiClient for Api {
        fn stream(
            &mut self,
            _: ApiRequest,
        ) -> std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>,
        > {
            panic!("zero provider unit")
        }
    }
    struct Executor {
        host: tools::ToolHost,
        fail: bool,
        attempts: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl ToolExecutor for Executor {
        fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
            Some(self.host.pin_snapshot())
        }
        fn available_tool_names(&self) -> Vec<String> {
            vec!["checkpoint".into(), "edit".into()]
        }
        fn registered_tool_effect(
            &self,
            name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            Some(self.host.pin_snapshot().describe_effect(name, input))
        }
        async fn execute_output(
            &self,
            _: &str,
            _: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
            Err(ToolError::new("unleased execution forbidden"))
        }
        async fn execute_authorized_output(
            &self,
            auth: &harness_contract::tool::ToolExecutionAuthorization,
            name: &str,
            input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(ToolError::new("injected exact checkpoint failure"));
            }
            let value = serde_json::from_str(input).map_err(|e| ToolError::new(format!("{e}")))?;
            self.host
                .pin_snapshot()
                .execute(auth, name, &value)
                .map(harness_contract::context::ToolOutputDraft::bounded_inline)
                .map_err(|e| ToolError::new(e.to_string()))
        }
    }
    #[tokio::test]
    async fn original_checkpoint_gate_keeps_parent_approval_and_execution_failures_closed() {
        for (mode, delegated, fail, allowed, attempts) in [
            (PermissionMode::WorkspaceWrite, true, false, true, 1),
            (PermissionMode::WorkspaceWrite, true, true, false, 1),
            (PermissionMode::ReadOnly, true, false, false, 0),
            (PermissionMode::ReadOnly, false, false, false, 0),
        ] {
            let root = tempfile::tempdir().unwrap();
            std::fs::write(root.path().join("a.txt"), b"original").unwrap();
            let executor = Executor {
                host: tools::ToolHost::builtin("r366-gate", root.path())
                    .with_authorization_lease_verifier(Arc::new(
                        crate::AuthorizationNegotiator::verify_lease_signature,
                    )),
                fail,
                attempts: AtomicUsize::new(0),
            };
            let prepared = plan(
                &executor.host,
                &[(
                    "write",
                    "edit",
                    serde_json::json!({"action":"write","path":"a.txt","content":"after"}),
                )],
            );
            let bus = crate::CowdEventBus::new();
            let _execution = bus.enter_execution_with_activity(
                crate::CowdExecutionContext {
                    execution_id: "r366-graph".into(),
                    session_id: "r366-session".into(),
                    turn_id: "r366-turn".into(),
                },
                None,
            );
            let mut session = Session::new();
            session.session_id = "r366-session".into();
            let mut runtime = ConversationRuntime::new(
                session,
                Api,
                executor,
                PermissionPolicy::new(mode),
                vec![],
            )
            .without_memory()
            .with_runtime_event_store(Arc::new(crate::RuntimeEventStore::for_test()))
            .with_cowd_event_bus(bus);
            if delegated {
                runtime = runtime.with_memory_identity("r366-producer", None, None, vec![]);
            }
            let decision = runtime
                .begin_turn_strategy("r366-turn", "write exact a.txt")
                .unwrap()
                .decision;
            let mut validation = prepared.validate_against_execution_decision(&decision);
            assert!(validation.requires_checkpoint);
            runtime
                .satisfy_tool_strategy_gates(
                    &decision,
                    &prepared,
                    &mut validation,
                    &crate::permissions::SharedPrompter::none(),
                )
                .await;
            assert_eq!(validation.allowed, allowed, "{:?}", validation.findings);
            assert_eq!(validation.checkpoint_created, allowed);
            if allowed {
                let checkpoints = std::fs::read_dir(root.path().join(".cowd/checkpoints"))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(checkpoints.len(), 1);
                let saved = checkpoints[0].path();
                let manifest: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(saved.join(".manifest.json")).unwrap())
                        .unwrap();
                assert_eq!(
                    manifest["scopes"],
                    serde_json::json!(["a.txt"]),
                    "actual gate authorization range must equal executed snapshot range"
                );
                assert_eq!(std::fs::read(saved.join("a.txt")).unwrap(), b"original");
            }

            assert_eq!(
                runtime.tool_executor.attempts.load(Ordering::SeqCst),
                attempts
            );
            assert_eq!(
                std::fs::read(root.path().join("a.txt")).unwrap(),
                b"original"
            );
            if fail {
                assert!(validation.findings.iter().any(|s| {
                    s.contains("checkpoint_creation_failed:injected exact checkpoint failure")
                }));
            }
            if !allowed && !fail {
                assert!(
                    validation
                        .findings
                        .iter()
                        .any(|s| s.contains("checkpoint_authorization_denied"))
                );
            }
        }
    }
}
