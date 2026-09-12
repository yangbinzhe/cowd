//! Shared, bound Agent leaf effect and durable receipt execution.
//! Native and Process callers use this owner; graph/container lifecycles stay in Runner.
use crate::execution_core::graph::{
    ExecutionCommitService, ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource,
};
use crate::{
    RuntimeExecutionHost, RuntimeToolExecutionOutcome, RuntimeToolExecutionRequest,
    RuntimeToolExecutionStatus, ToolError,
};
use harness_contract::tool::ToolEffectDescriptor;

pub(crate) fn observed_evidence_matches_requested_path(
    observed: &harness_contract::context::ObservedEvidence,
    requested_paths: &[String],
) -> bool {
    matches!(&observed.target, harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
        if requested_paths.contains(&scope.path.workspace_relative_path))
}

pub(crate) async fn execute_bound_agent_tool(
    host: &dyn RuntimeExecutionHost,
    commit_service: Option<&ExecutionCommitService>,
    path_identity_resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    scope_locks: &ScopeLockManager,
    request: &RuntimeToolExecutionRequest,
    descriptor: &ToolEffectDescriptor,
) -> Result<RuntimeToolExecutionOutcome, ToolError> {
    let tool_name = request.tool_name.as_str();
    let sequence = request.observation_wave_sequence;
    let requested = crate::governed_tool_plan::resource_scope_from_effect(descriptor);
    let bounded_sandbox_process = descriptor.spawns_process
        && descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Process
        && crate::delegated_tool_effect_is_bounded(descriptor);
    // AgentTask deliberately does not retain its broad resource locks while
    // awaiting the delegated child.  The concrete leaf effect therefore
    // acquires the same canonical locks used by graph ToolBatch nodes.  The
    // lease spans pre-image capture, execution and receipt materialization,
    // so neither the evidence snapshot nor the side effect can race another
    // in-process or persistent scoped executor.
    let lock_mode = if descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Write {
        ScopeLockMode::Write
    } else {
        ScopeLockMode::Read
    };
    let lock_paths = if bounded_sandbox_process {
        vec![".".to_string()]
    } else {
        requested.paths.clone()
    };
    let lock_requests = lock_paths
        .iter()
        .map(|path| {
            let identity = if bounded_sandbox_process {
                path_identity_resolver.resolve_existing(path)
            } else {
                path_identity_resolver.resolve_planned_file(path)
            };
            identity
                .map(|identity| ScopeLockRequest {
                    scope: ScopedResource::workspace_object(identity),
                    mode: lock_mode,
                })
                .map_err(|error| {
                    ToolError::new(format!(
                        "tool `{tool_name}` has an invalid scoped lock target `{path}`: {error}"
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let _scope_lock = if lock_requests.is_empty() {
        None
    } else {
        Some(
            scope_locks
                .acquire(lock_requests, None)
                .await
                .map_err(|error| {
                    ToolError::new(format!(
                        "tool `{tool_name}` could not acquire its scoped resource lease: {error}"
                    ))
                })?,
        )
    };
    let effect_state = commit_service
        .map(|service| service.begin_tool_effect(&request, &descriptor))
        .transpose()
        .map_err(|error| {
            ToolError::new(format!(
                "tool `{tool_name}` durable effect admission failed: {error}"
            ))
        })?
        .unwrap_or(crate::execution_core::graph::ToolEffectState::Fresh);
    let (mut outcome, fresh_execution) = match effect_state {
        crate::execution_core::graph::ToolEffectState::Completed(mut outcome) => {
            outcome.tool_use_id.clone_from(&request.tool_use_id);
            outcome.tool_name.clone_from(&request.tool_name);
            outcome.category = request.category;
            for evidence in &mut outcome.observed_evidence {
                evidence.provenance =
                    harness_contract::context::ObservedEvidenceProvenance::RetainedReplay;
            }
            (outcome, false)
        }
        crate::execution_core::graph::ToolEffectState::Uncertain => {
            return Err(ToolError::new(
                "tool effect is uncertain; non-idempotent execution was not replayed",
            ));
        }
        crate::execution_core::graph::ToolEffectState::Fresh
        | crate::execution_core::graph::ToolEffectState::NotRequired => {
            (host.execute_runtime_tool(&request).await, true)
        }
    };
    // Delegated ToolHost adapters must return typed observations together
    // with a successful receipt. Some compatibility adapters return only
    // raw structured output. In that case Runtime may mint exact-content
    // evidence solely when the output itself proves start=1, EOF coverage,
    // no truncation and a valid full-file digest. Requested paths and a
    // successful status alone are never evidence. Discovery tools must
    // provide their own typed observations; writes and failures are never
    // inferred here.
    if outcome.status == RuntimeToolExecutionStatus::Executed
        && outcome.observed_evidence.is_empty()
        && descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Read
    {
        let parsed = outcome
            .output
            .as_deref()
            .and_then(|output| serde_json::from_str::<serde_json::Value>(output).ok());
        if tool_name == "read_file" {
            if let Some(observed) = parsed.as_ref().and_then(|output| {
                path_identity_resolver
                    .observe_complete_read_tool_output(tool_name, output, sequence)
                    .ok()
                    .filter(|observed| {
                        observed_evidence_matches_requested_path(observed, &requested.paths)
                    })
            }) {
                outcome.observed_evidence.push(observed);
            }
        } else if tool_name == "read_many" {
            outcome.observed_evidence.extend(
                parsed
                    .as_ref()
                    .and_then(|output| output.get("results"))
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter(|item| {
                        item.get("status").and_then(serde_json::Value::as_str) == Some("success")
                    })
                    .filter_map(|item| item.get("output"))
                    .filter_map(|output| {
                        path_identity_resolver
                            .observe_complete_read_tool_output("read_file", output, sequence)
                            .ok()
                            .filter(|observed| {
                                observed_evidence_matches_requested_path(observed, &requested.paths)
                            })
                    }),
            );
        }
    }
    if fresh_execution {
        if let Some(commit_service) = commit_service {
            let committed = if descriptor.effect_kind
                == harness_contract::tool::ToolEffectKind::Read
            {
                commit_service.commit_readonly_tool_receipts(&[(request.clone(), outcome.clone())])
            } else {
                commit_service.commit_tool_effect(&request, &descriptor, &outcome)
            };
            if let Err(error) = committed {
                return Err(ToolError::new(format!(
                    "tool `{tool_name}` completed but durable receipt commit failed: {error}"
                )));
            }
        }
    }

    Ok(outcome)
}
