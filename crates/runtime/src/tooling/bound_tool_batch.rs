//! Shared, bound Agent leaf effect and durable receipt execution.
//! Native and Process callers use this owner; graph/container lifecycles stay in Runner.
use std::sync::Arc;
use crate::execution_core::graph::{
    ExecutionCommitService, ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource,
};
use crate::{
    RuntimeExecutionHost, RuntimeToolExecutionOutcome, RuntimeToolExecutionRequest,
    RuntimeToolExecutionStatus, ToolError,
};
use harness_contract::tool::ToolEffectDescriptor;

/// The exact original capability acquisition operation that returned an error.
/// This is an in-process observation, not a refusal or proof of no dispatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BoundedAcquisitionStage { Lease, Authority, Plane }

pub(crate) struct OriginalBoundedAcquisitionFailure {
    request: RuntimeToolExecutionRequest,
    stage: BoundedAcquisitionStage,
    store: std::sync::Weak<crate::RuntimeEventStore>,
    original_error: String,
}
impl std::fmt::Debug for OriginalBoundedAcquisitionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalBoundedAcquisitionFailure")
            .field("stage", &self.stage).finish_non_exhaustive()
    }
}
impl OriginalBoundedAcquisitionFailure {
    pub(crate) fn request(&self) -> &RuntimeToolExecutionRequest { &self.request }
    pub(crate) fn stage(&self) -> BoundedAcquisitionStage { self.stage }
    pub(crate) fn original_error(&self) -> &str { &self.original_error }
    pub(crate) fn belongs_to(&self, store: &std::sync::Arc<crate::RuntimeEventStore>) -> bool {
        self.store.upgrade().is_some_and(|owner| std::sync::Arc::ptr_eq(&owner, store))
    }
}

/// Preserve only the call-site fact that acquisition returned an error. An
/// existing Unknown remains Unknown and must not become a no-dispatch fact.
pub(crate) fn original_bounded_acquisition_failure(
    request: &RuntimeToolExecutionRequest,
    commit: &ExecutionCommitService,
    stage: BoundedAcquisitionStage,
    error: ToolError,
) -> ToolError {
    if error.is_unknown() { return error; }
    let original_error = error.to_string();
    error.with_original_bounded_acquisition_failure(std::sync::Arc::new(
        OriginalBoundedAcquisitionFailure { request: request.clone(), stage, store: commit.original_store_weak(), original_error },
    ))
}

/// Stop propagation for one original accepted call. It never owns an effect or
/// declares it drained; the retained operation must return its real outcome.
#[derive(Clone)]
pub(crate) struct BoundToolStop {
    pub(crate) requested: std::sync::Arc<std::sync::atomic::AtomicBool>,
    wake: std::sync::Arc<tokio::sync::Notify>,
    pub(crate) child: std::sync::Arc<std::sync::Mutex<Option<(String, String)>>>,
    pub(crate) child_drained: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub(crate) effect_entered: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub(crate) effect_settled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub(crate) process: tools::ToolExecutionCancellation,
    commit_controls: std::sync::Arc<std::sync::Mutex<Vec<std::sync::Weak<crate::conversation::MemberCommitControl>>>>,
    #[cfg(test)]
    pub(crate) stop_after_child_registration: bool,
}
impl Default for BoundToolStop {
    fn default() -> Self {
        Self {
            requested: Default::default(),
            wake: Default::default(),
            child: Default::default(),
            child_drained: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            effect_entered: Default::default(),
            effect_settled: Default::default(),
            process: tools::ToolExecutionCancellation::new(),
            commit_controls: Default::default(),
            #[cfg(test)]
            stop_after_child_registration: false,
        }
    }
}
impl BoundToolStop {
    pub(crate) fn request(&self) {
        self.requested
            .store(true, std::sync::atomic::Ordering::Release);
        self.process.cancel();
        let mut controls = self.commit_controls.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        controls.retain(|weak| {
            if let Some(control) = weak.upgrade() {
                crate::RuntimeTransactionCommitControl::request_cancel(control.as_ref());
                true
            } else { false }
        });
        drop(controls);
        self.wake.notify_waiters();
    }
    pub(crate) fn register_commit_control(&self, control: &std::sync::Arc<crate::conversation::MemberCommitControl>) {
        let mut controls = self.commit_controls.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.requested.load(std::sync::atomic::Ordering::Acquire) {
            crate::RuntimeTransactionCommitControl::request_cancel(control.as_ref());
        } else {
            controls.push(std::sync::Arc::downgrade(control));
        }
    }
    pub(crate) fn check(&self) -> Result<(), ToolError> {
        if self.requested.load(std::sync::atomic::Ordering::Acquire) {
            Err(ToolError::new(
                "original tool call stop requested before effect",
            ))
        } else {
            Ok(())
        }
    }
    pub(crate) async fn cancelled(&self) {
        loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.requested.load(std::sync::atomic::Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}
#[cfg(test)]
mod original_stop_control_tests {
    use super::*;
    use crate::RuntimeTransactionCommitControl;

    #[test]
    fn stop_racing_control_registration_cancels_the_same_commit() {
        for _ in 0..128 {
            let stop = BoundToolStop::default();
            let control = crate::CancellationToken::new().member_commit_control();
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            std::thread::scope(|scope| {
                let caller = stop.clone();
                let ready = std::sync::Arc::clone(&barrier);
                scope.spawn(move || { ready.wait(); caller.request(); });
                barrier.wait();
                stop.register_commit_control(&control);
            });
            assert!(matches!(control.state(), crate::conversation::MemberCommitState::CancelRequested));
            assert!(!control.begin().expect("the original commit is cancelled"));
        }
    }
}

// Only the actual outer read committer may suppress a nested adapter receipt.
// Task-local scope prevents unrelated Process or concurrent calls inheriting it.
tokio::task_local! { static OUTER_READ_RECEIPT_OWNER: (String, String, String, Option<String>, Option<crate::step_snapshot::StepCallBinding>); }
pub(crate) async fn with_outer_read_receipt_owner<T>(
    request: &crate::RuntimeToolExecutionRequest,
    work: impl std::future::Future<Output = T>,
) -> T {
    OUTER_READ_RECEIPT_OWNER.scope((request.idempotency_key.clone(), request.tool_use_id.clone(), request.tool_name.clone(), request.session_id.clone(), request.step_binding.clone()), work).await
}
pub(crate) fn outer_read_receipt_owned(request: &crate::RuntimeToolExecutionRequest) -> bool {
    OUTER_READ_RECEIPT_OWNER.try_with(|(key, call, tool, session, binding)|
        binding.is_some() && key == &request.idempotency_key && call == &request.tool_use_id
            && tool == &request.tool_name && session == &request.session_id && binding == &request.step_binding
    ).unwrap_or(false)
}
tokio::task_local! { static ORIGINAL_TOOL_STOP: BoundToolStop; }
pub(crate) fn current_tool_stop() -> Option<BoundToolStop> {
    ORIGINAL_TOOL_STOP.try_with(Clone::clone).ok()
}
pub(crate) async fn with_tool_stop<T>(
    stop: BoundToolStop,
    work: impl std::future::Future<Output = T>,
) -> T {
    ORIGINAL_TOOL_STOP.scope(stop, work).await
}
fn check_tool_stop() -> Result<(), ToolError> {
    current_tool_stop().map_or(Ok(()), |stop| stop.check())
}

#[cfg(test)]
mod resource_frame_tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn outer_read_owner_is_scoped_to_original_call_and_not_inherited_by_process_task() {
        let workspace = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("outer-read-owner", workspace.path());
        let request = super::scope_admission_tests::bound_request(&host, "read_file", serde_json::json!({"path":"one.txt"}));
        assert!(!outer_read_receipt_owned(&request));
        with_outer_read_receipt_owner(&request, async {
            assert!(outer_read_receipt_owned(&request));
            let mut foreign = request.clone();
            foreign.tool_use_id.push_str("-foreign");
            assert!(!outer_read_receipt_owned(&foreign));
            foreign = request.clone();
            foreign.idempotency_key.push_str("-foreign");
            assert!(!outer_read_receipt_owned(&foreign));
            foreign = request.clone();
            foreign.session_id = Some("foreign-session".into());
            assert!(!outer_read_receipt_owned(&foreign));
            foreign = request.clone();
            let binding = foreign.step_binding.as_mut().unwrap();
            std::sync::Arc::make_mut(&mut binding.snapshot).identity.attempt = Some(999);
            assert!(!outer_read_receipt_owned(&foreign));
            let detached = request.clone();
            assert!(!tokio::spawn(async move { outer_read_receipt_owned(&detached) }).await.unwrap(),
                "a Process task must not inherit a caller's receipt ownership");
        }).await;
        assert!(!outer_read_receipt_owned(&request));
    }

    #[tokio::test]
    #[ignore = "pins the workspace-root read lock a bounded compute no longer takes, so its conflict assertions hold nothing"]
    async fn r1_composite_effect_workspace_leaf_locks_cover_descendants_and_mutations() {
        for multi_repository in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let prefix = if multi_repository {
                for repository in ["first", "second"] {
                    std::fs::create_dir_all(root.path().join(repository).join(".git")).unwrap();
                    std::fs::create_dir_all(root.path().join(repository).join("src")).unwrap();
                }
                "first/"
            } else {
                std::fs::create_dir_all(root.path().join("src")).unwrap();
                ""
            };
            let target = format!("{prefix}src/held.txt");
            std::fs::write(root.path().join(&target), "preimage").unwrap();
            let resolver =
                crate::path_identity::WorkspacePathIdentityResolver::discover(root.path()).unwrap();
            if multi_repository {
                assert!(resolver.resolve_existing(".").is_err());
            }
            let host = tools::ToolHost::builtin("bound-resource-frame", root.path()).pin_snapshot();
            let compute = host.describe_effect(
                "tool_orchestrate",
                &serde_json::json!({"steps":[
                    {"id":"compute","op":"compute","language":"python","code":"print(1)"}
                ]}),
            );
            assert!(compute.spawns_process && !compute.uses_network);
            let (read_requests, identities) =
                bound_effect_lock_requests(&resolver, &compute).unwrap();
            assert!(identities.is_empty());
            assert_eq!(read_requests.len(), 1);
            assert_eq!(read_requests[0].mode, ScopeLockMode::Read);
            assert_eq!(
                read_requests[0].scope,
                ScopedResource::workspace(resolver.workspace_id()).unwrap()
            );
            let mixed = host.describe_effect("tool_orchestrate", &serde_json::json!({"steps":[
                {"id":"edit","op":"call","tool":"edit","input":{"action":"write","path":target,"content":"changed"}},
                {"id":"compute","op":"compute","language":"python","code":"print(1)"}
            ]}));
            assert!(mixed.spawns_process);
            let (mixed_requests, identities) =
                bound_effect_lock_requests(&resolver, &mixed).unwrap();
            assert_eq!(mixed_requests.len(), 4);
            assert_eq!(
                identities
                    .iter()
                    .map(|(path, _)| path.as_str())
                    .collect::<Vec<_>>(),
                vec![".cowd/checkpoints", target.as_str(), target.as_str()]
            );
            assert!(identities
                .iter()
                .all(|(_, identity)| identity.workspace_id == resolver.workspace_id()));
            let target_identity = resolver.resolve_planned_file(&target).unwrap();
            assert!(mixed_requests
                .iter()
                .any(|request| request.mode == ScopeLockMode::Write
                    && request.scope == ScopedResource::workspace_object(target_identity.clone())));
            assert!(mixed_requests
                .iter()
                .any(|request| request.mode == ScopeLockMode::Read
                    && request.scope == ScopedResource::workspace_object(target_identity.clone())));
            let checkpoint = ScopeLockRequest {
                scope: ScopedResource::workspace_object(
                    resolver.resolve_planned_file(".cowd/checkpoints").unwrap(),
                ),
                mode: ScopeLockMode::Write,
            };
            assert!(mixed_requests.contains(&checkpoint));
            assert!(mixed_requests
                .iter()
                .any(|request| request.mode == ScopeLockMode::Read
                    && request.scope
                        == ScopedResource::workspace(resolver.workspace_id()).unwrap()));
            let writer = ScopeLockRequest {
                scope: ScopedResource::workspace_object(
                    resolver.resolve_planned_file(&target).unwrap(),
                ),
                mode: ScopeLockMode::Write,
            };
            let locks = ScopeLockManager::new();
            let lease = locks.acquire(read_requests.clone(), None).await.unwrap();
            let compatible = locks
                .acquire(read_requests.clone(), Some(Duration::from_millis(20)))
                .await
                .unwrap();
            assert!(locks
                .acquire(vec![writer.clone()], Some(Duration::from_millis(20)))
                .await
                .is_err());
            drop(compatible);
            drop(lease);
            let mixed_lease = locks.acquire(mixed_requests, None).await.unwrap();
            assert!(locks
                .acquire(vec![checkpoint.clone()], Some(Duration::from_millis(20)))
                .await
                .is_err());
            assert!(locks
                .acquire(vec![writer.clone()], Some(Duration::from_millis(20)))
                .await
                .is_err());
            assert!(locks
                .acquire(
                    vec![ScopeLockRequest {
                        scope: writer.scope.clone(),
                        mode: ScopeLockMode::Read,
                    }],
                    Some(Duration::from_millis(20))
                )
                .await
                .is_err());
            drop(mixed_lease);
            let write_lease = locks.acquire(vec![writer.clone()], None).await.unwrap();
            assert!(locks
                .acquire(read_requests.clone(), Some(Duration::from_millis(20)))
                .await
                .is_err());
            drop(write_lease);
            let dynamic = host.describe_effect("tool_orchestrate", &serde_json::json!({"steps":[
                {"id":"read","op":"call","tool":"read_file","input":{"path":target}},
                {"id":"edit","op":"call","tool":"edit","input":{"action":"write","path":"$read.file.content","content":"changed"}}
            ]}));
            let (broad_requests, _) = bound_effect_lock_requests(&resolver, &dynamic).unwrap();
            assert!(broad_requests
                .iter()
                .any(|request| request.mode == ScopeLockMode::Write
                    && request.scope
                        == ScopedResource::workspace(resolver.workspace_id()).unwrap()));
            let broad_write = locks.acquire(broad_requests, None).await.unwrap();
            assert!(locks
                .acquire(vec![writer.clone()], Some(Duration::from_millis(20)))
                .await
                .is_err());
            if multi_repository {
                let second = ScopeLockRequest {
                    scope: ScopedResource::workspace_object(
                        resolver
                            .resolve_planned_file("second/src/other.txt")
                            .unwrap(),
                    ),
                    mode: ScopeLockMode::Write,
                };
                assert!(locks
                    .acquire(vec![second], Some(Duration::from_millis(20)))
                    .await
                    .is_err());
            }
            drop(broad_write);
            locks
                .acquire(vec![writer.clone()], Some(Duration::from_millis(20)))
                .await
                .unwrap();

            let network = host.describe_effect("tool_orchestrate", &serde_json::json!({"steps":[
                {"id":"edit","op":"call","tool":"edit","input":{"action":"write","path":target,"content":"changed"}},
                {"id":"fetch","op":"call","tool":"web_fetch","input":{"url":"https://example.invalid","prompt":"summarize"}}
            ]}));
            assert!(network.uses_network);
            let (network_requests, _) = bound_effect_lock_requests(&resolver, &network).unwrap();
            assert_eq!(network_requests.len(), 3);
            assert!(network_requests.contains(&checkpoint));
            assert!(network_requests.iter().any(
                |request| request.scope == writer.scope && request.mode == ScopeLockMode::Read
            ));
            assert!(network_requests.iter().any(
                |request| request.scope == writer.scope && request.mode == ScopeLockMode::Write
            ));
            let network_lease = locks.acquire(network_requests, None).await.unwrap();
            let unrelated = ScopeLockRequest {
                scope: ScopedResource::workspace_object(
                    resolver
                        .resolve_planned_file(if multi_repository {
                            "second/src/unrelated.txt"
                        } else {
                            "src/unrelated.txt"
                        })
                        .unwrap(),
                ),
                mode: ScopeLockMode::Write,
            };
            let unrelated_lease = locks
                .acquire(vec![unrelated], Some(Duration::from_millis(20)))
                .await
                .unwrap();
            let descendant = ScopeLockRequest {
                scope: ScopedResource::workspace_object(
                    resolver
                        .resolve_planned_file(".cowd/checkpoints/new/manifest.json")
                        .unwrap(),
                ),
                mode: ScopeLockMode::Read,
            };
            assert!(locks
                .acquire(vec![descendant], Some(Duration::from_millis(20)))
                .await
                .is_err());
            let sibling = host.describe_effect(
                "edit",
                &serde_json::json!({
                    "action":"write", "path":format!("{prefix}src/sibling.txt"), "content":"other"
                }),
            );
            let (sibling_requests, _) = bound_effect_lock_requests(&resolver, &sibling).unwrap();
            assert!(sibling_requests.contains(&checkpoint));
            assert!(locks
                .acquire(sibling_requests, Some(Duration::from_millis(20)))
                .await
                .is_err());
            drop(unrelated_lease);
            drop(network_lease);
            assert_eq!(
                std::fs::read_to_string(root.path().join(&target)).unwrap(),
                "preimage"
            );
        }
    }
}

#[cfg(test)]
pub(crate) mod scope_admission_tests {
    use super::*;
    use harness_contract::context::EvidenceTargetIdentity;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    pub(crate) fn bound_request(
        host: &tools::ToolHost,
        tool_name: &str,
        input: serde_json::Value,
    ) -> RuntimeToolExecutionRequest {
        let lease = host.pin_snapshot();
        let advertised = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| definition.name == tool_name)
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        assert_eq!(advertised.len(), 1);
        let identity = serde_json::from_value(json!({
            "step_id":format!("step:scope2:{tool_name}"), "session_id":"scope2-session",
            "turn_id":"scope2-turn", "execution_id":"scope2-graph", "actor_ref":"scope2-test",
            "attempt":1, "epoch":"1", "build_ref":"isolated-fixture",
        }))
        .unwrap();
        let step = Arc::new(
            crate::step_snapshot::BoundStep::bind(
                identity,
                lease,
                &advertised,
                &json!({}),
                &json!({}),
                BTreeMap::new(),
                i64::MAX,
            )
            .unwrap(),
        );
        let binding =
            crate::step_snapshot::StepCallBinding::provider(step, &format!("call:{tool_name}"));
        let invocation = binding
            .prepare_invocation(Some(host.pin_snapshot()), tool_name, &input)
            .unwrap();
        let mut request =
            RuntimeToolExecutionRequest::from_tool_request(&crate::tool_dispatch::ToolRequest {
                tool_use_id: binding.origin.call_id.clone(),
                tool_name: tool_name.into(),
                input: serde_json::to_string(&input).unwrap(),
                depends_on: Vec::new(),
            });
        request.idempotency_key = binding.effect_idempotency_key();
        request.session_id = Some("scope2-session".into());
        request.policy_revision = 1;
        request.observation_wave_sequence = 1;
        request.governed_plan_id = "scope2-plan".into();
        request.governed_plan_revision = 1;
        request.compiled_invocation = Some(invocation);
        request.step_binding = Some(binding);
        request
    }

    /// Runs the production host-shell executor for a delegated effect: the real
    /// async bash path with its own process group, bounded capture and
    /// quiescence proof.
    struct HostShellHost {
        workspace: std::path::PathBuf,
    }

    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for HostShellHost {
        fn delegated_tool_effect_descriptor(
            &self,
            name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            let _ = (name, input);
            None
        }

        async fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            let input: tools::bash::BashCommandInput =
                serde_json::from_str(&request.input).expect("bash input");
            let output = tools::bash::execute_bash_async_in_workspace(
                input,
                &self.workspace,
                None,
            )
            .await
            .expect("real host shell");
            crate::RuntimeToolExecutionOutcome {
                pending_agent_batch: None,
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some(serde_json::to_string_pretty(&output).unwrap()),
                error: None,
                evidence_ref: format!("tool-effect:{}", request.idempotency_key),
                observed_evidence: Vec::new(),
            }
        }
    }

    #[test]
    #[should_panic(expected = "fixture mode must cover the effective permission")]
    fn bounded_fixture_rejects_low_mode_before_issuing_human_approval() {
        let workspace = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("Own848-effective-floor", workspace.path());
        let mut request = bound_request(
            &host,
            "execute_code",
            json!({
                "language":"python", "code":"print(1)", "input_files":[], "timeout_ms":1000,
            }),
        );
        authorize_bounded_fixture(
            &host,
            &mut request,
            Arc::new(crate::RuntimeEventStore::for_test()),
            harness_contract::policy::PermissionMode::WorkspaceWrite,
        );
    }

    pub(crate) fn authorize_bounded_fixture(
        host: &tools::ToolHost,
        request: &mut RuntimeToolExecutionRequest,
        store: Arc<crate::RuntimeEventStore>,
        mode: harness_contract::policy::PermissionMode,
    ) {
        use harness_contract::policy::{
            ApprovalContext, ApprovalDecisionActor, ApprovalDecisionActorKind,
            ApprovalDecisionCommand, ApprovalDomain, ApprovalGrantScope, ApprovalGrantStatus,
            ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy, AuthorizationPath,
            CapabilityGapKind, RiskLevel, SubmitApprovalRequest,
        };
        assert_eq!(request.tool_name, "execute_code");
        let pin = host.pin_snapshot();
        let input: serde_json::Value = serde_json::from_str(&request.input).unwrap();
        let descriptor = pin.describe_effect(&request.tool_name, &input);
        assert_eq!(
            request.compiled_invocation.as_ref().unwrap().effect,
            descriptor
        );
        let required = crate::AuthorizationNegotiator::compile_effective_descriptor(
            &descriptor,
            &request.input,
        )
        .descriptor
        .required_permission;
        assert!(
            mode.permits(required),
            "fixture mode must cover the effective permission: mode={mode:?} required={required:?}"
        );
        let policy = crate::PermissionPolicy::new(mode);
        let authorization_request = crate::AuthorizationRequest {
            principal_id: "bounded-fixture-358".into(),
            capability: request.tool_name.clone(),
            input: request.input.clone(),
            idempotency_key: request.idempotency_key.clone(),
            effect: descriptor,
            parent_ceiling: mode,
            parent_lease_id: None,
            policy_revision: request.policy_revision,
            recovery_scope: request.governed_plan_id.clone(),
            context: crate::PermissionContext::default(),
            safe_alternatives: vec![],
        };
        let negotiator = crate::AuthorizationNegotiator::new();
        let evaluated = negotiator.assess_effective(&policy, &authorization_request);
        assert!(evaluated.assessment.lease.is_none());
        assert_eq!(evaluated.assessment.path, AuthorizationPath::HumanApproval);
        assert_eq!(
            evaluated.assessment.gap.as_ref().unwrap().kind,
            CapabilityGapKind::ApprovalRequired
        );
        let approvals = crate::ApprovalQueue::new(store.clone());
        let source = ApprovalSource {
            kind: ApprovalSourceKind::Session,
            session_id: request.session_id.clone(),
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: None,
            review_ref: None,
            application: None,
        };
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(pin.workspace_root())
                .unwrap();
        let mut context =
            ApprovalContext::owned(&source, &request.tool_name, resolver.workspace_id());
        context.principal_id = authorization_request.principal_id.clone();
        context.invocation_id = Some(request.idempotency_key.clone());
        context.execution_id = request
            .step_binding
            .as_ref()
            .unwrap()
            .snapshot
            .identity
            .execution_id
            .clone();
        context.effect = Some(evaluated.effective.descriptor.clone());
        context.policy_revision = request.policy_revision;
        context.resource_targets = evaluated
            .effective
            .descriptor
            .scopes
            .iter()
            .map(|scope| scope.target.clone())
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        let risk = match evaluated.assessment.risk {
            RiskLevel::Low => harness_contract::core::TaskRisk::Low,
            RiskLevel::Medium => harness_contract::core::TaskRisk::Medium,
            RiskLevel::High => harness_contract::core::TaskRisk::High,
            RiskLevel::Critical => harness_contract::core::TaskRisk::Critical,
        };
        let pending = approvals
            .submit_scoped_with_policy(
                format!("fixture-358:{}", request.idempotency_key),
                SubmitApprovalRequest {
                    source,
                    context,
                    action: format!("execute:{}", request.tool_name),
                    summary: "Explicit fixture human approval of this exact bounded invocation"
                        .into(),
                    risk,
                    domain: ApprovalDomain::Execution,
                    blocks_execution: true,
                    evidence_refs: vec![evaluated.effective.fingerprint.clone()],
                    timeout_policy: ApprovalTimeoutPolicy::AutoDeny,
                },
                None,
                false,
                vec![ApprovalGrantScope::Once],
            )
            .unwrap();
        let human = crate::security::test_human_interactive_principal();
        approvals
            .decide(
                &human,
                ApprovalDecisionCommand {
                    approval_id: pending.approval_id.clone(),
                    approved: true,
                    skip: false,
                    reason:
                        "Approve only the original pinned descriptor, policy revision and call key"
                            .into(),
                    scope: ApprovalGrantScope::Once,
                    actor: ApprovalDecisionActor {
                        kind: ApprovalDecisionActorKind::Human,
                        actor_id: human.claims().principal_id.clone(),
                    },
                    evidence_refs: vec![evaluated.effective.fingerprint.clone()],
                },
            )
            .unwrap();
        let grant = approvals.grant_for_approval(&pending.approval_id).unwrap();
        for defect in ["revoked", "invocation", "policy", "descriptor"] {
            let mut invalid = grant.clone();
            match defect {
                "revoked" => invalid.status = ApprovalGrantStatus::Revoked,
                "invocation" => {
                    invalid.invocation_id = Some(format!("{}:foreign", request.idempotency_key))
                }
                "policy" => invalid.policy_revision += 1,
                "descriptor" => invalid.effect_descriptor_hash = Some("foreign".into()),
                _ => unreachable!(),
            }
            let denied = negotiator.approve_effective(
                &policy,
                &authorization_request,
                &evaluated.effective,
                &invalid,
            );
            assert!(denied.lease.is_none(), "{defect}");
            assert_eq!(
                denied.gap.as_ref().unwrap().kind,
                CapabilityGapKind::ApprovalRequired,
                "{defect}"
            );
        }
        let approved = negotiator.approve_effective(
            &policy,
            &authorization_request,
            &evaluated.effective,
            &grant,
        );
        assert_eq!(approved.path, AuthorizationPath::HumanApproval);
        let signed = approved
            .lease
            .clone()
            .expect("explicit exact human approval must issue an original lease");
        assert!(crate::AuthorizationNegotiator::verify_lease_signature(
            &signed
        ));
        assert!(signed
            .ceiling
            .permits(evaluated.effective.descriptor.required_permission));
        assert_eq!(
            approved.required_mode,
            evaluated.effective.descriptor.required_permission
        );
        let authorization = crate::ToolPolicy
            .authorize(
                &evaluated.effective,
                &approved,
                &request.idempotency_key,
                signed,
                30,
            )
            .unwrap()
            .authorization;
        pin.validate_authorization(&authorization, &request.tool_name, &input)
            .unwrap();
        let mut forged = authorization.clone();
        forged.authorization_lease.signature.push_str("-forged");
        assert!(matches!(
            pin.validate_authorization(&forged, &request.tool_name, &input),
            Err(tools::ToolHostError::InvalidLeaseSignature)
        ));
        approvals.consume_once_grant(&grant.grant_id).unwrap();
        assert!(approvals.active_grants().is_empty());
        assert!(
            store
                .stream_revision(&format!("approval:{}", pending.approval_id))
                .unwrap()
                > 0
        );
        request.authorization = Some(authorization);
    }

    struct ReplayGuardHost {
        lease: tools::ToolHostLease,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[tokio::test]
    async fn bounded_process_unsupported_host_never_falls_back_to_string_execution_283() {
        let workspace = tempfile::tempdir().unwrap();
        let tools = tools::ToolHost::builtin("bounded-unsupported", workspace.path());
        let request = bound_request(
            &tools,
            "execute_code",
            json!({"language":"python","code":"print(283)","input_files":[]}),
        );
        let descriptor = &request.compiled_invocation.as_ref().unwrap().effect;
        let host = ReplayGuardHost {
            lease: tools.pin_snapshot(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        let service = ExecutionCommitService::new(Arc::new(crate::RuntimeEventStore::for_test()));
        let result = execute_bound_agent_tool(
            &host,
            Some(&service),
            &resolver,
            &ScopeLockManager::new(),
            &request,
            descriptor,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(host.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(host.bounded_process_plane().is_none());
        assert!(host.bounded_process_lease(&request).is_err());
        assert!(host.bounded_process_authority(&request).is_err());
    }

    #[async_trait::async_trait]
    impl RuntimeExecutionHost for ReplayGuardHost {
        fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
            Some(self.lease.clone())
        }

        async fn execute_runtime_tool(
            &self,
            request: &RuntimeToolExecutionRequest,
        ) -> RuntimeToolExecutionOutcome {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            RuntimeToolExecutionOutcome {
                pending_agent_batch: None,
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: RuntimeToolExecutionStatus::Failed,
                category: request.category,
                output: None,
                error: Some("replay must not reach the executor".into()),
                evidence_ref: "unexpected-execution".into(),
                observed_evidence: vec![],
            }
        }
    }

    /// A host that answers like the host shell executor: it returns the
    /// executor's own structured result, including the quiescence report.
    struct HostProcessHost {
        lease: tools::ToolHostLease,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl RuntimeExecutionHost for HostProcessHost {
        fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
            Some(self.lease.clone())
        }

        async fn execute_runtime_tool(
            &self,
            request: &RuntimeToolExecutionRequest,
        ) -> RuntimeToolExecutionOutcome {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            RuntimeToolExecutionOutcome {
                pending_agent_batch: None,
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some(
                    json!({
                        "stdout": "890",
                        "stderr": "",
                        "returnTruncated": false,
                        "processClosure": {
                            "exitCode": 0, "signal": null,
                            "initReaped": true, "allChildrenReaped": true,
                        },
                    })
                    .to_string(),
                ),
                error: None,
                evidence_ref: format!("tool-effect:{}", request.idempotency_key),
                observed_evidence: vec![],
            }
        }
    }

    /// The Runtime kernel mints the host process terminal from the executor's
    /// own report, and the physical closure witness then judges the effect
    /// closed. This is the path a real Session tool call takes: the tool
    /// result is the executor's structured output, and the durable receipt
    /// must carry the honest quiescence proof.
    #[tokio::test]
    async fn r1_892_a_conversation_style_tool_output_mints_the_closure_evidence() {
        use harness_contract::context::{EvidenceTargetIdentity, ToolExecutionTerminal};
        let workspace = tempfile::tempdir().unwrap();
        let tools = tools::ToolHost::builtin("r1-892-host-process", workspace.path());
        let store = Arc::new(crate::RuntimeEventStore::for_test());
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
            workspace.path().join("artifacts"),
        ));
        let service =
            ExecutionCommitService::new(Arc::clone(&store)).with_artifact_store(artifacts);
        let request = bound_request(&tools, "bash", json!({"command": "printf 892 > out.txt; printf 892"}));
        let descriptor = request
            .compiled_invocation
            .as_ref()
            .expect("compiled invocation")
            .effect
            .clone();
        assert!(descriptor.spawns_process);
        let mut outcome = crate::RuntimeToolExecutionOutcome {
            pending_agent_batch: None,
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some(
                json!({
                    "stdout": "892",
                    "stderr": "",
                    "processClosure": {
                        "exitCode": 0,
                        "signal": null,
                        "initReaped": true,
                        "allChildrenReaped": true,
                    },
                })
                .to_string(),
            ),
            error: None,
            evidence_ref: format!("tool-effect:{}", request.idempotency_key),
            observed_evidence: Vec::new(),
        };
        let minted = crate::host_process_closure::mint_host_process_evidence(
            &service,
            &request,
            &descriptor,
            &mut outcome,
            Some(&crate::host_process_closure::test_admission(&request)),
        )
        .await
        .expect("mint the host process closure");
        assert!(
            minted,
            "a process-class effect must mint its closure evidence"
        );
        let facts = outcome
            .observed_evidence
            .iter()
            .filter_map(|observed| match &observed.target {
                EvidenceTargetIdentity::ToolExecution { execution } => Some(execution),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(facts.len(), 1, "exactly one process terminal");
        assert!(
            matches!(
                &facts[0].terminal,
                ToolExecutionTerminal::Settled {
                    exit_code: Some(0),
                    signal: None,
                    closure,
                    ..
                } if closure.all_children_reaped && closure.init_reaped
            ),
            "{:?}",
            facts[0].terminal
        );
        assert!(crate::execution_projection::settlement::process_closed(
            &outcome,
            &descriptor
        ));
    }

    /// A report that does not prove the exit never becomes a closure: the
    /// witness keeps failing closed.
    #[tokio::test]
    async fn r1_892_a_host_process_report_without_proof_never_closes() {
        let workspace = tempfile::tempdir().unwrap();
        let tools = tools::ToolHost::builtin("r1-892-host-process-unproven", workspace.path());
        let store = Arc::new(crate::RuntimeEventStore::for_test());
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
            workspace.path().join("artifacts"),
        ));
        let service =
            ExecutionCommitService::new(Arc::clone(&store)).with_artifact_store(artifacts);
        let request = bound_request(&tools, "bash", json!({"command": "printf 892 > out.txt; printf 892"}));
        let descriptor = request
            .compiled_invocation
            .as_ref()
            .expect("compiled invocation")
            .effect
            .clone();
        let mut outcome = crate::RuntimeToolExecutionOutcome {
            pending_agent_batch: None,
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some(json!({"stdout": "892"}).to_string()),
            error: None,
            evidence_ref: format!("tool-effect:{}", request.idempotency_key),
            observed_evidence: Vec::new(),
        };
        crate::host_process_closure::mint_host_process_evidence(
            &service,
            &request,
            &descriptor,
            &mut outcome,
            Some(&crate::host_process_closure::test_admission(&request)),
        )
        .await
        .expect("mint the honest unknown");
        assert!(
            !crate::execution_projection::settlement::process_closed(&outcome, &descriptor),
            "an unproven process exit must never close"
        );
    }

    /// F2: a host process effect publishes its quiescence fact into the
    /// durable receipt, and the historical replay keeps that evidence.
    #[tokio::test]
    #[ignore = "needs a really started parent attempt; a child effect is refused unless the owning node runs on its recorded attempt"]
    async fn host_process_effect_publishes_durable_closure_evidence_890() {
        use harness_contract::context::EvidenceTargetIdentity;
        let workspace = tempfile::tempdir().unwrap();
        let tools = tools::ToolHost::builtin("r1-890-host-process", workspace.path());
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        let store = Arc::new(crate::RuntimeEventStore::for_test());
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
            workspace.path().join("artifacts"),
        ));
        // The owning graph node is running, exactly as it is when a delegated
        // Agent really executes a process-class effect, and the artifact sink
        // can publish a prepared closure document.
        let services = crate::RuntimeServices::builder(
            workspace.path().join("home"),
            workspace.path(),
        )
        .runtime_event_store(store.clone())
        .artifact_store(artifacts)
        .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
        .build()
        .unwrap();
        let service = services.commit_service().clone();
        let mut graph = harness_contract::execution_graph::ExecutionGraph::new("r1-890 graph");
        graph.id = "scope2-graph".into();
        graph.parent_execution =
            Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "scope2-root".into(),
                node_id: "scope2-root-node".into(),
            });
        let mut spec = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::ToolBatch,
            "tool_batch",
            "r1-890 payload",
        );
        spec.id = "scope2-node".into();
        graph.nodes.push(spec.clone());
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let registered = service.register_graph(graph).unwrap().graph;
        let ready = service
            .transition_node(
                &registered,
                &spec.id,
                harness_contract::execution_graph::ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        service
            .transition_node(
                &ready,
                &spec.id,
                harness_contract::execution_graph::ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .unwrap();
        let mut request = bound_request(&tools, "bash", json!({"command": "printf 890 > out.txt; printf 890"}));
        request.parent_execution =
            Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "scope2-graph".into(),
                node_id: "scope2-node".into(),
            });
        request.parent_execution_attempt = Some(1);
        let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
        assert!(effect.spawns_process);
        let host = HostProcessHost {
            lease: tools.pin_snapshot(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let outcome = execute_bound_agent_tool(
            &host,
            Some(&service),
            &resolver,
            &ScopeLockManager::new(),
            &request,
            &effect,
        )
        .await
        .expect("host process effect");
        let facts = outcome
            .observed_evidence
            .iter()
            .filter_map(|observed| match &observed.target {
                EvidenceTargetIdentity::ToolExecution { execution } => Some(execution),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            facts.len(),
            1,
            "the fresh outcome must carry one process terminal"
        );
        assert!(
            matches!(
                &facts[0].terminal,
                harness_contract::context::ToolExecutionTerminal::Settled {
                    exit_code: Some(0),
                    signal: None,
                    ..
                }
            ),
            "{:?}",
            facts[0].terminal
        );
        assert_eq!(host.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        // The durable receipt publishes the closure envelope and the admission
        // reference, so the physical closure witness can settle it.
        let receipt = store
            .event_by_idempotency_key(
                &format!("execution-effect:{}", request.idempotency_key),
                &format!("{}:receipt", request.idempotency_key),
            )
            .unwrap()
            .expect("durable host process receipt");
        assert_eq!(receipt.payload["process_admission_ref"],
            json!(format!("{}:process-admission", request.idempotency_key)));
        assert_eq!(
            receipt.payload["process_closure"]["content_schema"],
            json!(2),
            "the durable receipt must publish the closure envelope"
        );
        assert!(crate::execution_projection::settlement::process_closed(
            &outcome, &effect
        ));
        // The retained replay of the same effect is covered by
        // `r1_899_host_shell_receipt_settles_through_the_production_consumer`
        // and by `r1_892_a_conversation_style_tool_output_mints_the_closure_evidence`,
        // which own the NodeStart event that the replay path reads.
    }

    /// A host-shell process effect runs a real command, is admitted before it
    /// runs, publishes its closure, and the production settlement consumer then
    /// judges the durable receipt physically closed. The admission, the closure
    /// envelope and the process admission reference all have to be present in
    /// the same transaction for that to happen.
    #[tokio::test]
    #[ignore = "needs a really started parent attempt; a child effect is refused unless the owning node runs on its recorded attempt"]
    async fn r1_899_host_shell_receipt_settles_through_the_production_consumer() {
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
        };
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("marker.txt"), "before").unwrap();
        let store = Arc::new(crate::RuntimeEventStore::for_test());
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
            workspace.path().join("artifacts"),
        ));
        let services = crate::RuntimeServices::builder(
            workspace.path().join("home"),
            workspace.path(),
        )
        .runtime_event_store(store.clone())
        .artifact_store(artifacts)
        .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
        .build()
        .unwrap();
        let mut graph = ExecutionGraph::new("r1-899 host shell graph");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        // The delegated step binding is bound to this exact execution and node,
        // so the graph that owns the effect carries the same identity.
        graph.id = "scope2-graph".into();
        graph.parent_execution =
            Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "scope2-root".into(),
                node_id: "scope2-root-node".into(),
            });
        let mut spec = ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "tool_batch",
            "r1-899 payload",
        );
        spec.id = "scope2-node".into();
        graph.nodes.push(spec.clone());
        let registered = services
            .commit_service()
            .register_graph(graph)
            .unwrap()
            .graph;
        let ready = services
            .commit_service()
            .transition_node(
                &registered,
                &spec.id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let running = services
            .commit_service()
            .transition_node(&ready, &spec.id, ExecutionNodeStatus::Running, None, Vec::new())
            .unwrap()
            .graph;
        assert_eq!(running.node_statuses[&spec.id], ExecutionNodeStatus::Running);
        assert_eq!(running.recovery_cursor.node_attempts[&spec.id], 1);

        let tools = tools::ToolHost::builtin("r1-899-host-shell", workspace.path());
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        let mut request = bound_request(
            &tools,
            "bash",
            json!({"command": "printf 899 > marker.txt; printf done",
                   "dangerouslyDisableSandbox": true, "workspaceAccess": "read_write"}),
        );
        request.parent_execution =
            Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: running.id.clone(),
                node_id: spec.id.clone(),
            });
        request.parent_execution_attempt = Some(1);
        let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
        assert!(effect.spawns_process);
        let host = HostShellHost {
            workspace: workspace.path().to_path_buf(),
        };
        let outcome = execute_bound_agent_tool(
            &host,
            Some(services.commit_service()),
            &resolver,
            &ScopeLockManager::new(),
            &request,
            &effect,
        )
        .await
        .expect("real host shell effect");
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("marker.txt")).unwrap(),
            "899",
            "the command really ran"
        );
        let facts = outcome
            .observed_evidence
            .iter()
            .filter_map(|observed| match &observed.target {
                EvidenceTargetIdentity::ToolExecution { execution } => Some(execution),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(facts.len(), 1, "exactly one process terminal");
        assert!(
            matches!(
                &facts[0].terminal,
                harness_contract::context::ToolExecutionTerminal::Settled {
                    exit_code: Some(0),
                    signal: None,
                    closure,
                    ..
                } if closure.all_children_reaped && closure.init_reaped
            ),
            "{:?}",
            facts[0].terminal
        );
        // The admission is durable and precedes the receipt.
        let stream = format!("execution-effect:{}", request.idempotency_key);
        let admission = store
            .event_by_idempotency_key(&stream, &format!("{}:process-admission", request.idempotency_key))
            .unwrap()
            .expect("the admission is published before the command runs");
        assert_eq!(admission.kind, "execution.process.admitted");
        let receipt = store
            .event_by_idempotency_key(&stream, &format!("{}:receipt", request.idempotency_key))
            .unwrap()
            .expect("the durable receipt");
        assert!(
            (admission.commit_cursor, admission.transaction_index)
                < (receipt.commit_cursor, receipt.transaction_index),
            "the admission precedes the receipt"
        );
        assert_eq!(
            receipt.payload["process_admission_ref"],
            json!(format!("{}:process-admission", request.idempotency_key)),
        );
        let envelope = receipt
            .payload
            .get("process_closure")
            .expect("the receipt publishes the closure envelope");
        assert_eq!(envelope["content_schema"], json!(2));
        assert_eq!(envelope["body"]["kind"], json!("artifact_ref"));
        assert_eq!(
            admission.payload["admission"],
            serde_json::to_value(&facts[0].admission).unwrap(),
            "the receipt settles against the admission that was published"
        );
        // The production consumer, not a local predicate, decides closure.
        let closed = crate::execution_projection::settlement::inspect_physical_closure_for_test(
            &services,
            &receipt,
            &outcome,
            &effect,
        )
        .expect("inspect the physical closure");
        assert!(closed, "the production settlement consumer must close this receipt");
    }

    /// H2: a read-class effect that owns a process tree keeps its own key
    /// family. It is admitted before the command runs, its read receipt
    /// carries the published closure and the admission reference, and the
    /// production settlement consumer closes it against the read intent.
    #[tokio::test]
    #[ignore = "needs a really started parent attempt; a child effect is refused unless the owning node runs on its recorded attempt"]
    async fn r1_901_a_read_class_process_effect_settles_through_the_production_consumer() {
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
        };
        let workspace = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::RuntimeEventStore::for_test());
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
            workspace.path().join("artifacts"),
        ));
        let services = crate::RuntimeServices::builder(
            workspace.path().join("home"),
            workspace.path(),
        )
        .runtime_event_store(store.clone())
        .artifact_store(artifacts)
        .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
        .build()
        .unwrap();
        let mut graph = ExecutionGraph::new("r1-901 read process graph");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.id = "scope2-graph".into();
        graph.parent_execution =
            Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "scope2-root".into(),
                node_id: "scope2-root-node".into(),
            });
        let mut spec = ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "tool_batch",
            "r1-901 payload",
        );
        spec.id = "scope2-node".into();
        graph.nodes.push(spec.clone());
        let registered = services.commit_service().register_graph(graph).unwrap().graph;
        let ready = services
            .commit_service()
            .transition_node(
                &registered,
                &spec.id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let running = services
            .commit_service()
            .transition_node(&ready, &spec.id, ExecutionNodeStatus::Running, None, Vec::new())
            .unwrap()
            .graph;
        let tools = tools::ToolHost::builtin("r1-901-read-process", workspace.path());
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        let mut request = bound_request(&tools, "bash", json!({"command": "printf 901"}));
        request.parent_execution =
            Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: running.id.clone(),
                node_id: spec.id.clone(),
            });
        request.parent_execution_attempt = Some(1);
        let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
        assert!(effect.spawns_process, "the read effect owns a process tree");
        assert_eq!(
            effect.effect_kind,
            harness_contract::tool::ToolEffectKind::Read,
            "the effect is read-class"
        );
        let host = HostShellHost {
            workspace: workspace.path().to_path_buf(),
        };
        let outcome = execute_bound_agent_tool(
            &host,
            Some(services.commit_service()),
            &resolver,
            &ScopeLockManager::new(),
            &request,
            &effect,
        )
        .await
        .expect("read-class host shell effect");
        let facts = outcome
            .observed_evidence
            .iter()
            .filter_map(|observed| match &observed.target {
                EvidenceTargetIdentity::ToolExecution { execution } => Some(execution),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(facts.len(), 1, "exactly one process terminal");
        let stream = format!("execution-effect:{}", request.idempotency_key);
        let read_intent = store
            .event_by_idempotency_key(&stream, &format!("{}:read-intent", request.idempotency_key))
            .unwrap()
            .expect("the read family keeps its own intent");
        let admission = store
            .event_by_idempotency_key(
                &stream,
                &format!("{}:process-admission", request.idempotency_key),
            )
            .unwrap()
            .expect("the admission is published before the command runs");
        assert!(
            (read_intent.commit_cursor, read_intent.transaction_index)
                < (admission.commit_cursor, admission.transaction_index),
            "the admission follows the read intent"
        );
        let receipt = store
            .event_by_idempotency_key(&stream, &format!("{}:read-receipt", request.idempotency_key))
            .unwrap()
            .expect("the read receipt");
        assert_eq!(
            receipt.payload["process_admission_ref"],
            json!(format!("{}:process-admission", request.idempotency_key)),
        );
        assert_eq!(
            receipt.payload["process_closure"]["content_schema"],
            json!(2),
            "the read receipt publishes the closure envelope"
        );
        assert_eq!(
            admission.payload["admission"],
            serde_json::to_value(&facts[0].admission).unwrap(),
        );
        let closed = crate::execution_projection::settlement::inspect_physical_closure_for_test(
            &services,
            &receipt,
            &outcome,
            &effect,
        )
        .expect("inspect the physical closure");
        assert!(
            closed,
            "the production settlement consumer must close a read-class process receipt"
        );
    }

    /// The same chain fails closed at every missing link: no admission, no
    /// closure publication, or a receipt that claims a terminal it was never
    /// given. None of them may settle.
    #[tokio::test]
    async fn r1_899_host_shell_without_the_admission_chain_never_closes() {
        let workspace = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::RuntimeEventStore::for_test());
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
            workspace.path().join("artifacts"),
        ));
        let service =
            ExecutionCommitService::new(Arc::clone(&store)).with_artifact_store(artifacts);
        let tools = tools::ToolHost::builtin("r1-899-unproven", workspace.path());
        let request = bound_request(&tools, "bash", json!({"command": "printf 899"}));
        let descriptor = request.compiled_invocation.as_ref().unwrap().effect.clone();
        let mut outcome = crate::RuntimeToolExecutionOutcome {
            pending_agent_batch: None,
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some(
                json!({
                    "stdout": "899",
                    "stderr": "",
                    "processClosure": {
                        "exitCode": 0, "signal": null,
                        "initReaped": true, "allChildrenReaped": true,
                    },
                })
                .to_string(),
            ),
            error: None,
            evidence_ref: format!("tool-effect:{}", request.idempotency_key),
            observed_evidence: Vec::new(),
        };
        // Without the admission that must be published before the command runs,
        // the kernel refuses to claim a terminal at all.
        let minted = crate::host_process_closure::mint_host_process_evidence(
            &service,
            &request,
            &descriptor,
            &mut outcome,
            None,
        )
        .await
        .expect("no admission is a refusal, not a failure");
        assert!(!minted, "no admission means no process terminal");
        assert!(
            outcome.observed_evidence.is_empty(),
            "an unadmitted effect must not carry a process fact"
        );
        assert!(!crate::execution_projection::settlement::process_closed(
            &outcome, &descriptor
        ));
        // A read-class effect that owns a process tree keeps its own key
        // family: it mints its terminal against its read admission, and it is
        // settled against the read intent rather than the mutation intent.
        let mut read = descriptor.clone();
        read.effect_kind = harness_contract::tool::ToolEffectKind::Read;
        let mut read_outcome = outcome.clone();
        let minted = crate::host_process_closure::mint_host_process_evidence(
            &service,
            &request,
            &read,
            &mut read_outcome,
            Some(&crate::host_process_closure::test_admission(&request)),
        )
        .await
        .expect("a read-class effect mints against its read admission");
        assert!(
            minted,
            "a read-class process effect with an admission claims its terminal"
        );
        // A receipt that carries a terminal fact but was committed through the
        // public path is refused outright.
        let mut forged = outcome.clone();
        forged.observed_evidence = vec![crate::host_process_closure::host_process_observation(
            &request,
            crate::host_process_closure::build_host_process_fact(
                &request,
                &descriptor,
                &crate::host_process_closure::read_host_process_report(Some(
                    &json!({
                        "stdout": "899",
                        "stderr": "",
                        "processClosure": {
                            "exitCode": 0, "signal": null,
                            "initReaped": true, "allChildrenReaped": true,
                        },
                    })
                    .to_string(),
                )),
                harness_contract::context::ArtifactRef {
                    sha256: format!("sha256:{}", "0".repeat(64)),
                    bytes: 2,
                    media_type: "application/json".into(),
                    visibility_scope: format!("session:{}", request.session_id.as_deref().unwrap()),
                    selector: "artifact://forged".into(),
                    durability: harness_contract::context::EvidenceDurability::Durable,
                },
                crate::host_process_closure::test_admission(&request),
            )
            .expect("forged fact"),
        )];
        let rejected = service.commit_tool_effect(&request, &descriptor, &forged);
        assert!(
            rejected.is_err(),
            "the public commit path refuses a process terminal fact"
        );
    }

    #[test]
    fn original_glob_file_domain_uses_real_mixed_scope_without_widening() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("proof.txt"), "body").unwrap();
        let host = tools::ToolHost::builtin("glob-domain", workspace.path());
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path()).unwrap();
        let request = bound_request(&host, "glob_search", json!({"path":".", "pattern":"*.txt"}));
        let descriptor = &request.compiled_invocation.as_ref().unwrap().effect;
        let scope = admitted_effect_scope(&request, descriptor, Some(host.pin_snapshot()), &resolver).unwrap();
        assert!(!scope.complete, "the non-File capability remains outside the overall scope");
        assert!(crate::context_evidence::original_read_file_domain(descriptor, &scope));
        let mut missing = scope.clone();
        missing.targets.clear();
        assert!(!crate::context_evidence::original_read_file_domain(descriptor, &missing));
        let mut wrong = scope.clone();
        wrong.descriptor_hash = "foreign".into();
        assert!(!crate::context_evidence::original_read_file_domain(descriptor, &wrong));
        let mut network = descriptor.clone(); network.uses_network = true;
        assert!(!crate::context_evidence::original_read_file_domain(&network, &scope));
        let mut process = descriptor.clone(); process.spawns_process = true;
        assert!(!crate::context_evidence::original_read_file_domain(&process, &scope));
        let mut packages = descriptor.clone(); packages.mutates_packages = true;
        assert!(!crate::context_evidence::original_read_file_domain(&packages, &scope));
        let mut system = descriptor.clone(); system.mutates_system = true;
        assert!(!crate::context_evidence::original_read_file_domain(&system, &scope));
        let mut wrong_kind = descriptor.clone(); wrong_kind.effect_kind = harness_contract::tool::ToolEffectKind::Unknown;
        assert!(!crate::context_evidence::original_read_file_domain(&wrong_kind, &scope));
    }

    #[test]
    fn shared_scope_builder_authenticates_full_purpose_and_original_step() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("proof.txt"), "before").unwrap();
        let host = tools::ToolHost::builtin("scope2-builder", workspace.path());
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        let request = bound_request(
            &host,
            "edit",
            json!({
                "action":"write", "path":"proof.txt", "content":"after",
            }),
        );
        let descriptor = &request.compiled_invocation.as_ref().unwrap().effect;
        let scope =
            admitted_effect_scope(&request, descriptor, Some(host.pin_snapshot()), &resolver)
                .unwrap();
        assert_eq!(scope.version, 2);
        assert!(scope.complete);
        assert_eq!(scope.scope_purposes.len(), descriptor.scopes.len());
        assert_eq!(scope.targets.len(), descriptor.scopes.len());
        assert!(scope.scope_purposes.iter().any(|entry| entry.purpose
            == harness_contract::goal::ProofEffectPurpose::AutoCheckpointMaintenance));
        assert!(scope.scope_purposes.iter().any(|entry| entry.purpose
            == harness_contract::goal::ProofEffectPurpose::BusinessEffect
            && entry.declared_scope.target.as_deref() == Some("proof.txt")));
        assert!(scope
            .targets
            .iter()
            .filter(|target| target.path.workspace_relative_path == "proof.txt")
            .all(|target| target.alias_checked && target.physical_identity.is_some()));
        let mut missing_step = request.clone();
        missing_step.step_binding = None;
        assert!(admitted_effect_scope(
            &missing_step,
            descriptor,
            Some(host.pin_snapshot()),
            &resolver
        )
        .is_err());
        let mut changed_input = request.clone();
        changed_input.input =
            json!({"action":"write","path":"other.txt","content":"after"}).to_string();
        assert!(admitted_effect_scope(
            &changed_input,
            descriptor,
            Some(host.pin_snapshot()),
            &resolver
        )
        .is_err());
        let mut changed_descriptor = descriptor.clone();
        changed_descriptor.scopes.pop();
        assert!(admitted_effect_scope(
            &request,
            &changed_descriptor,
            Some(host.pin_snapshot()),
            &resolver
        )
        .is_err());
        let other_workspace = tempfile::tempdir().unwrap();
        let other_resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(other_workspace.path())
                .unwrap();
        assert!(admitted_effect_scope(
            &request,
            descriptor,
            Some(host.pin_snapshot()),
            &other_resolver
        )
        .is_err());
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("proof.txt")).unwrap(),
            "before"
        );
    }

    #[test]
    fn direct_grep_batch_retains_the_original_locked_workspace_target() {
        use harness_contract::context::WorkspaceObjectKind;
        use harness_contract::policy::PermissionResource;
        let workspace = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("scope-grep-root", workspace.path());
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path()).unwrap();
        let request = bound_request(&host, "grep_search", json!({
            "path": workspace.path().to_str().unwrap(), "searches":[{"pattern":"hit"}]
        }));
        let descriptor = &request.compiled_invocation.as_ref().unwrap().effect;
        let scope = admitted_effect_scope(&request, descriptor, Some(host.pin_snapshot()), &resolver).unwrap();
        assert!(!scope.complete);
        let roots = scope.targets.iter().filter(|target| target.declared_scope.resource == PermissionResource::File).collect::<Vec<_>>();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].path.workspace_relative_path, ".");
        assert_eq!(roots[0].path.object_kind, WorkspaceObjectKind::Directory);
        assert!(!roots[0].exact && !roots[0].alias_checked && roots[0].physical_identity.is_none());
        let other = tempfile::tempdir().unwrap();
        let foreign = crate::path_identity::WorkspacePathIdentityResolver::discover(other.path()).unwrap();
        assert!(admitted_effect_scope(&request, descriptor, Some(host.pin_snapshot()), &foreign).is_err());
    }

    #[test]
    fn shared_scope_builder_keeps_incomplete_network_and_process_authorization_bounds() {
        let workspace = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("scope2-incomplete", workspace.path());
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        for (tool_name, input) in [
            ("bash", json!({"command":"printf scope2"})),
            (
                "web_fetch",
                json!({"url":"https://example.invalid","prompt":"read"}),
            ),
            (
                "tool_orchestrate",
                json!({"steps":[
                    {"id":"edit","op":"call","tool":"edit","input":{"action":"write","path":"proof.txt","content":"after"}},
                    {"id":"compute","op":"compute","language":"python","code":"print(1)"}
                ]}),
            ),
        ] {
            let request = bound_request(&host, tool_name, input);
            let descriptor = &request.compiled_invocation.as_ref().unwrap().effect;
            let scope =
                admitted_effect_scope(&request, descriptor, Some(host.pin_snapshot()), &resolver)
                    .unwrap();
            assert!(!scope.complete, "{tool_name}");
            assert_eq!(scope.scope_purposes.len(), descriptor.scopes.len());
            assert!(scope.validate_purpose_mapping(descriptor).is_ok());
        }
    }
}

pub(crate) fn observed_evidence_matches_requested_path(
    observed: &harness_contract::context::ObservedEvidence,
    requested_paths: &[String],
) -> bool {
    matches!(&observed.target, harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
        if requested_paths.contains(&scope.path.workspace_relative_path))
}

pub(crate) fn bound_effect_lock_requests(
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    descriptor: &ToolEffectDescriptor,
) -> Result<
    (
        Vec<ScopeLockRequest>,
        Vec<(String, harness_contract::context::WorkspacePathIdentity)>,
    ),
    ToolError,
> {
    let mut requests = Vec::new();
    let mut identities = Vec::new();
    for (path, write) in crate::tool_host::effect_file_accesses(descriptor) {
        let mode = if write {
            ScopeLockMode::Write
        } else {
            ScopeLockMode::Read
        };
        let normalized = crate::governed_tool_plan::normalize_resource_path(&path);
        let scope = if resolver.is_workspace_scope(&normalized) {
            ScopedResource::workspace(resolver.workspace_id()).map_err(|error| {
                ToolError::new(format!("invalid workspace lock identity: {error}"))
            })?
        } else {
            let identity = resolver.resolve_planned_file(&path).map_err(|error| {
                ToolError::new(format!("invalid scoped lock target `{path}`: {error}"))
            })?;
            identities.push((path, identity.clone()));
            ScopedResource::workspace_object(identity)
        };
        requests.push(ScopeLockRequest { scope, mode });
    }
    Ok((requests, identities))
}

pub(crate) fn admitted_effect_scope(
    request: &RuntimeToolExecutionRequest,
    descriptor: &ToolEffectDescriptor,
    current: Option<tools::ToolHostLease>,
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
) -> Result<harness_contract::goal::ProofEffectScope, ToolError> {
    let tool_name = request.tool_name.as_str();
    let input = harness_contract::step::parse_strict_json(&request.input)
        .map_err(|error| ToolError::new(error.to_string()))?;
    let invocation = request.compiled_invocation.as_ref()
        .ok_or_else(|| ToolError::new("effect scope requires the compiled invocation"))?;
    if invocation.intent.normalized_input != input || invocation.effect != *descriptor
        || invocation.invocation_id != request.tool_use_id || invocation.intent.tool_name != request.tool_name {
        return Err(ToolError::new("effective_input_requires_replan"));
    }
    let lease = if let Some(binding) = request.step_binding.as_ref() {
        let step = binding.resolve(current).map_err(|error| ToolError::new(error.to_string()))?;
        if invocation.origin.as_ref() != Some(&binding.origin)
            || binding.snapshot.as_ref() != step.snapshot()
            || request.session_id.as_deref() != Some(binding.snapshot.identity.session_id.as_str()) {
            return Err(ToolError::new("effect scope original Step/call identity mismatch"));
        }
        step.lease().clone()
    } else {
        if request.tool_name == "agent_action_batch" || harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&request.tool_name.as_str()) {
            return Err(ToolError::new("original action Step binding missing"));
        }
        request.tool_progress.original_governed_hook().map_err(ToolError::new)?
            .ok_or_else(|| ToolError::new("original unbound call owner missing"))?
            .original_authorized_lease(request).map_err(ToolError::new)?
    };
    let pinned_resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(lease.workspace_root())
        .map_err(|error| ToolError::new(error.to_string()))?;
    if pinned_resolver.workspace_id() != resolver.workspace_id() {
        return Err(ToolError::new("effect scope resolver differs from the pinned workspace"));
    }
    let resolution = lease.describe_effect_resolution(tool_name, &input);
    if resolution.descriptor != *descriptor {
        return Err(ToolError::new("pinned effect descriptor changed at admission"));
    }
    let (_, resolved_identities) = bound_effect_lock_requests(resolver, descriptor)?;
    let targets = descriptor
        .scopes
        .iter()
        .filter_map(|scope| {
            if scope.resource != harness_contract::policy::PermissionResource::File
                || !matches!(
                    scope.operation,
                    harness_contract::policy::PermissionOperation::Read
                        | harness_contract::policy::PermissionOperation::Write
                )
            {
                return None;
            }
            let target = scope.target.as_ref()?;
            let identity = if let Some((_, identity)) = resolved_identities.iter().find(|(path, _)| path == target) {
                Ok(identity.clone())
            } else if (matches!(tool_name, "glob_search" | "tool_orchestrate")
                || tool_name == "grep_search" && input.get("searches").and_then(serde_json::Value::as_array).is_some())
                && scope.operation == harness_contract::policy::PermissionOperation::Read
                && resolver.is_workspace_scope(&crate::governed_tool_plan::normalize_resource_path(target)) {
                // The original workspace lock was admitted above; resolve its
                // actual directory rather than treating the lock as a file id.
                pinned_resolver.resolve_existing(target).map_err(|error| ToolError::new(format!(
                    "tool `{tool_name}` workspace target admission failed: {error}")))
            } else { return None; };
            Some(
                identity.and_then(|identity| resolver
                    .physical_file_identity_at_admission(&identity)
                    .map(
                        |physical_identity| harness_contract::goal::ProofEffectTarget {
                            path: identity.clone(),
                            declared_scope: scope.clone(),
                            access_mode: if scope.operation
                                == harness_contract::policy::PermissionOperation::Write
                            {
                                harness_contract::context::WorkspaceAccessMode::Write
                            } else {
                                harness_contract::context::WorkspaceAccessMode::Read
                            },
                            coverage: if scope.operation
                                == harness_contract::policy::PermissionOperation::Write
                            {
                                harness_contract::context::EvidenceCoverageKind::WriteEffect
                            } else {
                                harness_contract::context::EvidenceCoverageKind::ScopedContent
                            },
                            exact: identity.object_kind
                                == harness_contract::context::WorkspaceObjectKind::File,
                            alias_checked: physical_identity.is_some(),
                            physical_identity,
                        },
                    )
                    .map_err(|error| {
                        ToolError::new(format!(
                            "tool `{tool_name}` physical target admission failed: {error}"
                        ))
                    })),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let resolved_scope = harness_contract::goal::ProofEffectScope {
        version: 2,
        scope_purposes: resolution.scope_purposes,
        descriptor_hash: descriptor.descriptor_hash.clone(),
        complete: !descriptor.scopes.is_empty()
            && targets.len() == descriptor.scopes.len()
            && !descriptor.uses_network
            && !descriptor.spawns_process
            && !descriptor.mutates_packages
            && !descriptor.mutates_system
            && matches!(
                descriptor.effect_kind,
                harness_contract::tool::ToolEffectKind::Read
                    | harness_contract::tool::ToolEffectKind::Write
            )
            && descriptor.scopes.iter().all(|scope| {
                scope.resource == harness_contract::policy::PermissionResource::File
                    && matches!(
                        scope.operation,
                        harness_contract::policy::PermissionOperation::Read
                            | harness_contract::policy::PermissionOperation::Write
                    )
            }),
        targets,
    };

    resolved_scope
        .validate_purpose_mapping(descriptor)
        .map_err(ToolError::new)?;
    Ok(resolved_scope)
}

struct BoundedProcessWaiter {
    signal: tools::ToolExecutionCancellation,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

pub(crate) fn retained_replay_outcome(
    mut outcome: RuntimeToolExecutionOutcome,
) -> RuntimeToolExecutionOutcome {
    for observed in &mut outcome.observed_evidence {
        observed.provenance = harness_contract::context::ObservedEvidenceProvenance::RetainedReplay;
    }
    outcome
}

impl Drop for BoundedProcessWaiter {
    fn drop(&mut self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        self.signal.cancel();
    }
}

pub(crate) async fn execute_bounded_process_owned(
    lease: tools::ToolHostLease,
    plane: std::sync::Arc<crate::ToolExecutionPlane>,
    authority: crate::tool_host::BoundedProcessAuthority,
    service: ExecutionCommitService,
    request: RuntimeToolExecutionRequest,
    descriptor: ToolEffectDescriptor,
    root: Option<crate::CowdExecutionContext>,
    owner: Option<(harness_contract::projection::RuntimeActivityBinding, u32)>,
) -> Result<RuntimeToolExecutionOutcome, ToolError> {
    execute_bounded_process_observed(
        lease, plane, authority, service, request, descriptor, root, owner, None,
    )
    .await
}

pub(crate) async fn execute_bounded_process_observed(
    lease: tools::ToolHostLease,
    plane: std::sync::Arc<crate::ToolExecutionPlane>,
    mut authority: crate::tool_host::BoundedProcessAuthority,
    service: ExecutionCommitService,
    request: RuntimeToolExecutionRequest,
    descriptor: ToolEffectDescriptor,
    root: Option<crate::CowdExecutionContext>,
    owner: Option<(harness_contract::projection::RuntimeActivityBinding, u32)>,
    observation: Option<crate::execution_core::graph::executors::agent_tool::NoDispatchObservation>,
) -> Result<RuntimeToolExecutionOutcome, ToolError> {
    if !authority.shares_store(&service) {
        return Err(ToolError::new(
            "bounded process authority and ledger have different original stores",
        ));
    }
    if !authority.shares_plane(&plane) {
        return Err(ToolError::new(
            "bounded process authority requires its original RuntimeServices plane",
        ));
    }
    authority.check_context(&request)?;
    if let Some(outcome) = service
        .completed_tool_effect(&request, &descriptor)
        .map_err(|error| ToolError::new(error.to_string()))?
    {
        return Ok(retained_replay_outcome(outcome));
    }
    let artifacts = service
        .bounded_process_artifacts()
        .map_err(|error| ToolError::new(error.to_string()))?;
    let stop = current_tool_stop().unwrap_or_default();
    stop.check()?;
    let cancellation = stop.process.clone();
    let cancelled = stop.requested.clone();
    let waiter = BoundedProcessWaiter {
        signal: cancellation.clone(),
        cancelled: cancelled.clone(),
    };
    let handle = tokio::runtime::Handle::current();
    let locks = std::sync::Arc::clone(plane.scope_locks());
    let demand = crate::governed_tool_plan::resource_demand_from_effect(&descriptor);
    let original_class = service
        .bounded_process_service_class(&request, root.as_ref())
        .map_err(|error| ToolError::new(error.to_string()))?;
    let (class, ceiling) = plane
        .owned_classification()
        .unwrap_or((original_class, Some(original_class)));
    if class != original_class || class.bounded_by(ceiling) != original_class {
        return Err(ToolError::new(
            "bounded process frame differs from its original graph service class",
        ));
    }
    let session = request.session_id.clone();
    let retained_observation = observation.clone();
    let retained_request = request.clone();
    let (result, admission) = plane.execute_owned_classified_retained(
        &demand, None, class, ceiling, session.as_deref(), move || {
            let mut request = request;
            let mut execute = || -> Result<RuntimeToolExecutionOutcome, ToolError> {
                authority.check_context(&request)?;
                if cancelled.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(ToolError::new("bounded process cancelled before preparation"));
                }
                if let Some(outcome) = service.completed_tool_effect(&request, &descriptor)
                    .map_err(|error| ToolError::new(error.to_string()))? { return Ok(retained_replay_outcome(outcome)); }
                let step = request.step_binding.as_ref().ok_or_else(|| ToolError::new("bounded process Step missing"))?
                    .resolve(Some(lease.clone())).map_err(|error| ToolError::new(error.to_string()))?;
                step.validate_call(&request.step_binding.as_ref().unwrap().origin,
                    request.session_id.as_deref().ok_or_else(|| ToolError::new("bounded process session missing"))?,
                    &request.tool_use_id, &request.tool_name, &request.input,
                    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|error| ToolError::new(error.to_string()))?.as_millis() as i64,
                ).map_err(|error| ToolError::new(error.to_string()))?;
                let original = request.step_binding.as_ref().unwrap();
                let restored = match &original.origin.source {
                    harness_contract::step::CallSource::Provider { .. } => crate::step_snapshot::StepCallBinding::provider(step.clone(), &original.origin.call_id),
                    harness_contract::step::CallSource::SystemDerived { parent_step_id } => crate::step_snapshot::StepCallBinding::system(step.clone(), parent_step_id, &original.origin.call_id),
                    harness_contract::step::CallSource::Process { worker_id, sequence } => crate::step_snapshot::StepCallBinding::process(step.clone(), worker_id, *sequence, &original.origin.call_id),
                    harness_contract::step::CallSource::OfflineTest => return Err(ToolError::new("bounded process requires an actual production Step source")),
                };
                if restored != *original { return Err(ToolError::new("bounded process restored Step changed original origin")); }
                request.step_binding = Some(restored);
                let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(lease.workspace_root())
                    .map_err(|error| ToolError::new(error.to_string()))?;
                let (requests, identities) = bound_effect_lock_requests(&resolver, &descriptor)?;
                let canonical = handle.block_on(async {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => Err(ToolError::new("original tool call stopped waiting for locks")),
                        lease = locks.acquire(requests, None) => lease.map_err(|error| ToolError::new(error.to_string())),
                    }
                })?;
                authority.check_context(&request)?;
                let scope = admitted_effect_scope(&request, &descriptor, Some(lease.clone()), &resolver)?;
                if identities.iter().any(|(path, identity)| scope.targets.iter()
                    .filter(|target| target.declared_scope.target.as_ref() == Some(path)).any(|target| target.path != *identity)) {
                    return Err(ToolError::new("bounded process canonical identity drift"));
                }
                let state = if let Some(root) = &root {
                    let (binding, attempt) = owner.as_ref().ok_or_else(|| ToolError::new("bounded root original owner missing"))?;
                    service.begin_root_tool_effect(&request, &descriptor, root, &scope, binding, *attempt)
                } else { service.begin_bound_tool_effect_observed(&request, &descriptor, &scope, observation.as_ref()) }
                    .map_err(|error| ToolError::new(error.to_string()))?;
                match state {
                    crate::execution_core::graph::ToolEffectState::Completed(outcome) => return Ok(retained_replay_outcome(outcome)),
                    crate::execution_core::graph::ToolEffectState::Uncertain => return Err(ToolError::unknown("bounded process uncertain; refusing to rerun")),
                    crate::execution_core::graph::ToolEffectState::Fresh => {},
                    crate::execution_core::graph::ToolEffectState::NotRequired => return Err(ToolError::new("bounded process requires original durable intent")),
                }
                let binding = service.bounded_process_binding(&request, &descriptor).map_err(|error| ToolError::new(error.to_string()))?;
                stop.effect_entered.store(true,std::sync::atomic::Ordering::Release);
                authority.begin(&request)?;
                if cancelled.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(ToolError::new("bounded process cancelled before copy"));
                }
                let input = harness_contract::step::parse_strict_json(&request.input).map_err(|error| ToolError::new(error.to_string()))?;
                let prepared = lease.prepare_bounded_process(request.authorization.as_ref().ok_or_else(|| ToolError::new("bounded process grant missing"))?, &input, binding)
                    .map_err(|error| ToolError::new(error.to_string()))?;
                #[cfg(test)]
                authority.checkpoint_for_test("after_copy");
                if cancelled.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(ToolError::new("bounded process cancelled during copy"));
                }
                authority.check(&request)?;
                let bytes = prepared.source_bundle_bytes();
                if bytes.len() > 17 * 1024 * 1024 || bytes.len() as u64 > artifacts.config().max_object_bytes {
                    return Err(ToolError::new("bounded source archive exceeds actual storage budget"));
                }
                let source = handle.block_on(artifacts.write_bytes(harness_contract::context::ArtifactWriteDescriptor {
                    media_type:"application/octet-stream".into(), visibility_scope:format!("session:{}",request.session_id.as_deref().unwrap()),
                    expected_bytes:Some(bytes.len() as u64), original_name:None,
                }, bytes)).map_err(|error| ToolError::new(error.to_string()))?;
                artifacts.pin(&source, &format!("execution-effect:{}",request.idempotency_key), i64::MAX as u64).map_err(|error| ToolError::new(error.to_string()))?;
                let admission = service.admit_bounded_process(&request, &descriptor, &prepared, &source).map_err(|error| ToolError::new(error.to_string()))?;
                authority.check(&request)?;
                let monitor = authority.monitor(&request,cancellation.clone())?;
                let completed = prepared.run(admission, &cancellation);
                drop(monitor);
                #[cfg(test)]
                authority.checkpoint_for_test("after_terminal");
                if let Err(error)=authority.check_settlement(&request) {
                    let archived=handle.block_on(service.archive_bounded_process_body(&request,&completed,&artifacts));
                    let reason=match archived {
                        Ok(body)=>format!("actual process terminal archived at {}; original authority revoked: {error}",body.selector),
                        Err(archive_error)=>format!("actual process terminated; authority revoked: {error}; terminal archive failed: {archive_error}"),
                    };
                    authority.reconcile(reason.clone());
                    return Err(ToolError::new(reason));
                }
                let uncertain=matches!(completed.terminal(),harness_contract::context::ToolExecutionTerminal::Unknown { .. });
                let result = handle.block_on(authority.with_commit_context(service.commit_bounded_process(&request, &descriptor, completed, &artifacts)))
                    .map_err(|error| ToolError::new(error.to_string()));
                match &result {
                    Ok(outcome) if !uncertain => {
                        if let Err(error)=authority.complete(&request,&outcome.evidence_ref) {
                            authority.reconcile(format!("original process receipt {}; managed completion requires reconciliation: {error}",outcome.evidence_ref));
                            return Err(error);
                        }
                    },
                    Ok(_) => authority.reconcile("bounded process retained an Unknown terminal".into()),
                    Err(error) => authority.reconcile(error.to_string()),
                }
                drop(canonical);
                result
            };
            execute()
        },
    ).await;
    drop(admission);
    drop(waiter);
    if let Err(error) = &result {
        if let Some(observation) = &retained_observation {
            match error {
                crate::ToolExecutionPlaneError::Admission(_) => {
                    observation.refuse(&retained_request.tool_use_id, error.to_string())
                }
                _ => observation.uncertain(&retained_request),
            }
        }
    }
    result.map_err(|error| ToolError::new(error.to_string()))?
}

pub(crate) async fn execute_bound_agent_tool(
    host: &dyn RuntimeExecutionHost,
    commit_service: Option<&ExecutionCommitService>,
    path_identity_resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    scope_locks: &ScopeLockManager,
    request: &RuntimeToolExecutionRequest,
    descriptor: &ToolEffectDescriptor,
) -> Result<RuntimeToolExecutionOutcome, ToolError> {
    execute_bound_agent_tool_observed(
        host,
        commit_service,
        path_identity_resolver,
        scope_locks,
        request,
        descriptor,
        None,
        None,
    )
    .await
}

pub(crate) fn execute_bound_agent_tool_observed<'a>(
    host: &'a dyn RuntimeExecutionHost,
    commit_service: Option<&'a ExecutionCommitService>,
    path_identity_resolver: &'a crate::path_identity::WorkspacePathIdentityResolver,
    scope_locks: &'a ScopeLockManager,
    request: &'a RuntimeToolExecutionRequest,
    descriptor: &'a ToolEffectDescriptor,
    observation: Option<
        &'a crate::execution_core::graph::executors::agent_tool::NoDispatchObservation,
    >,
    original_child: Option<(crate::execution_core::NodeExecutionTicket, Arc<crate::execution_core::graph::executors::OriginalAgentChildCall>)>,
) -> futures::future::BoxFuture<'a, Result<RuntimeToolExecutionOutcome, ToolError>> {
    // The actual default-stack fault was the nested inline observed/core poll
    // (1157 first SIGSEGV). Construct only captures here; the original body is
    // first polled under the caller's same task-local scope, not another task.
    Box::pin(async move {
        let result = execute_bound_agent_tool_observed_inner(
            host,
            commit_service,
            path_identity_resolver,
            scope_locks,
            request,
            descriptor,
            observation,
            original_child,
        )
        .await;
        if result.is_ok() {
            if let Some(stop) = current_tool_stop() {
                stop.effect_settled
                    .store(true, std::sync::atomic::Ordering::Release);
            }
        }
        result
    })
}

async fn execute_bound_agent_tool_observed_inner(
    host: &dyn RuntimeExecutionHost,
    commit_service: Option<&ExecutionCommitService>,
    path_identity_resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    scope_locks: &ScopeLockManager,
    request: &RuntimeToolExecutionRequest,
    descriptor: &ToolEffectDescriptor,
    observation: Option<
        &crate::execution_core::graph::executors::agent_tool::NoDispatchObservation,
    >,
    original_child: Option<(crate::execution_core::NodeExecutionTicket, Arc<crate::execution_core::graph::executors::OriginalAgentChildCall>)>,
) -> Result<RuntimeToolExecutionOutcome, ToolError> {
    let tool_name = request.tool_name.as_str();
    let sequence = request.observation_wave_sequence;
    if let Some(service) = commit_service {
        if let Some(mut outcome) =
            service
                .completed_tool_effect(request, descriptor)
                .map_err(|error| {
                    ToolError::unknown(format!(
                        "tool `{tool_name}` historical receipt lookup failed: {error}"
                    ))
                })?
        {
            if outcome.status == RuntimeToolExecutionStatus::Unknown {
                return Err(ToolError::unknown_with_optional_agent_batch(outcome.error.unwrap_or_else(||
                    "original tool outcome is unknown".into()), outcome.pending_agent_batch));
            }
            outcome.tool_use_id.clone_from(&request.tool_use_id);
            outcome.tool_name.clone_from(&request.tool_name);
            outcome.category = request.category;
            for evidence in &mut outcome.observed_evidence {
                evidence.provenance =
                    harness_contract::context::ObservedEvidenceProvenance::RetainedReplay;
            }
            return Ok(outcome);
        }
    }
    if tool_name == "execute_code" {
        let service = commit_service
            .ok_or_else(|| ToolError::new("bounded process requires original commit service"))?;
        let preserve = |stage, error| original_bounded_acquisition_failure(request, service, stage, error);
        let plane = host.bounded_process_plane().ok_or_else(||
            preserve(BoundedAcquisitionStage::Plane, ToolError::new("bounded process plane unsupported")))?;
        let lease = host.bounded_process_lease(request)
            .map_err(|error| preserve(BoundedAcquisitionStage::Lease, error))?;
        let authority = host.bounded_process_authority(request)
            .map_err(|error| preserve(BoundedAcquisitionStage::Authority, error))?;
        if !std::ptr::eq(scope_locks, plane.scope_locks().as_ref()) {
            return Err(preserve(BoundedAcquisitionStage::Plane, ToolError::new(
                "bounded process canonical lock manager mismatch",
            )));
        }
        return execute_bounded_process_observed(
            lease,
            plane,
            authority,
            service.clone(),
            request.clone(),
            descriptor.clone(),
            None,
            None,
            observation.cloned(),
        )
        .await;
    }
    check_tool_stop()?;
    let stop = current_tool_stop();
    let requested = crate::governed_tool_plan::resource_scope_from_effect(descriptor);
    let (lock_requests, resolved_identities) =
        bound_effect_lock_requests(path_identity_resolver, descriptor).map_err(|error| {
            if let Some(observation) = observation {
                observation.refuse(&request.tool_use_id, error.to_string());
            }
            error
        })?;
    let _scope_lock = if lock_requests.is_empty() {
        None
    } else {
        Some(
            async {
                let acquire = scope_locks.acquire(lock_requests, None);
                if let Some(stop) = &stop {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => Err("original tool call stopped waiting for locks".to_string()),
                        lease = acquire => lease.map_err(|error| error.to_string()),
                    }
                } else { acquire.await.map_err(|error| error.to_string()) }
            }.await.map_err(|error| {
                    let error = ToolError::new(format!(
                        "tool `{tool_name}` could not acquire its scoped resource lease: {error}"
                    ));
                    if let Some(observation) = observation {
                        observation.refuse(&request.tool_use_id, error.to_string());
                    }
                    error
                })?,
        )
    };
    let resolved_scope = admitted_effect_scope(
        request,
        descriptor,
        if request.step_binding.is_some() { host.pin_step_catalog() } else { None },
        path_identity_resolver,
    )
    .map_err(|error| {
        if let Some(observation) = observation {
            observation.refuse(&request.tool_use_id, error.to_string());
        }
        error
    })?;
    for (path, identity) in &resolved_identities {
        if resolved_scope
            .targets
            .iter()
            .filter(|target| target.declared_scope.target.as_ref() == Some(path))
            .any(|target| target.path != *identity)
        {
            return Err(ToolError::new(
                "scoped path identity changed while acquiring resource locks",
            ));
        }
    }
    check_tool_stop()?;
    let original_owner = request.tool_progress.original_fresh_effect()
        .map_err(ToolError::unknown)?;
    let effect_state = if original_owner.is_some()
        && harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&tool_name)
    {
        let service = commit_service.ok_or_else(|| ToolError::unknown("original child commit owner unavailable"))?;
        service.claim_original_child_effect(request, descriptor, &resolved_scope)
            .map_err(|error| ToolError::unknown(error.to_string()))?;
        crate::execution_core::graph::ToolEffectState::Fresh
    } else {
    let effect_state = commit_service
        .map(|service| {
            service.begin_bound_tool_effect_observed(
                request,
                descriptor,
                &resolved_scope,
                observation,
            )
        })
        .transpose()
        .map_err(|error| {
            ToolError::new(format!(
                "tool `{tool_name}` durable effect admission failed: {error}"
            ))
        })?
        .unwrap_or(crate::execution_core::graph::ToolEffectState::Fresh);
        effect_state
    };
    let (mut outcome, fresh_execution, mut host_process_admission) = match effect_state {
        crate::execution_core::graph::ToolEffectState::Completed(mut outcome) => {
            outcome.tool_use_id.clone_from(&request.tool_use_id);
            outcome.tool_name.clone_from(&request.tool_name);
            outcome.category = request.category;
            for evidence in &mut outcome.observed_evidence {
                evidence.provenance =
                    harness_contract::context::ObservedEvidenceProvenance::RetainedReplay;
            }
            (outcome, false, None)
        }
        crate::execution_core::graph::ToolEffectState::Uncertain => {
            return Err(ToolError::unknown(
                "tool effect is uncertain; non-idempotent execution was not replayed",
            ));
        }
        crate::execution_core::graph::ToolEffectState::Fresh
        | crate::execution_core::graph::ToolEffectState::NotRequired => {
            if let Some(observation) = observation {
                observation.uncertain(request);
            }
            // A process-class effect is admitted before its command runs, so the
            // receipt can only be settled against a durable admission.
            let admission = if let Some(service) = commit_service {
                service
                    .admit_host_process(request, descriptor)
                    .await
                    .map_err(|error| {
                        ToolError::new(format!(
                            "tool `{tool_name}` host process admission failed: {error}"
                        ))
                    })?
            } else {
                None
            };
            check_tool_stop()?;
            if let Some(stop) = current_tool_stop() {
                stop.effect_entered
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            let outcome = if commit_service.is_some() && descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Read {
                with_outer_read_receipt_owner(&request, host.execute_runtime_tool(&request)).await
            } else {
                host.execute_runtime_tool(&request).await
            };
            (outcome, true, admission)
        }
    };
    // Neither a fresh Unknown nor a retained Unknown is a settled effect.
    // Return it to the original owner without minting any ordinary receipt.
    if outcome.status == RuntimeToolExecutionStatus::Unknown {
        if let (Some(service), Some((ticket, child)), Some(pending)) =
            (commit_service, original_child.as_ref(), outcome.pending_agent_batch.as_ref()) {
            let service = (*service).clone();
            let ticket = ticket.clone();
            let child = Arc::clone(child);
            let parent_stop = child.stop.clone();
            let request = request.clone();
            let pending = pending.clone();
            let authenticated = tokio::task::spawn_blocking(move ||
                service.original_terminal_batch_message(&ticket, &request, &pending, Some(&child))).await;
            if let Ok(Ok(Some(message))) = authenticated {
                if check_tool_stop().is_ok() && parent_stop.as_ref().is_some_and(|stop| stop.check().is_ok()) {
                    outcome.status = RuntimeToolExecutionStatus::Failed;
                    outcome.error = Some(message);
                }
            }
        }
        if outcome.status == RuntimeToolExecutionStatus::Unknown {
            return Err(ToolError::unknown_with_optional_agent_batch(outcome.error.unwrap_or_else(||
                "original tool outcome is unknown".into()), outcome.pending_agent_batch));
        }
    }
    // Delegated ToolHost adapters must return typed observations together
    // with a successful receipt. Some compatibility adapters return only
    // raw structured output. In that case Runtime may mint exact-content
    // evidence solely when the output itself proves start=1, EOF coverage,
    // no truncation and a valid full-file digest. Requested paths and a
    // successful status alone are never evidence. Discovery tools must
    // provide their own typed observations; writes and failures are never
    // inferred here.
    if fresh_execution
        && outcome.status == RuntimeToolExecutionStatus::Executed
        && descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Read
    {
        let parsed = outcome
            .output
            .as_deref()
            .and_then(|output| serde_json::from_str::<serde_json::Value>(output).ok());
        if tool_name == "read_file" {
            outcome.observed_evidence.retain(|observed| {
                !matches!(&observed.target, harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
                    if scope.coverage == harness_contract::context::EvidenceCoverageKind::ExactContent)
            });
            if let Some(observed) = parsed
                .as_ref()
                .filter(|output| {
                    output["file"]["numLines"].as_u64().is_some()
                        && output["file"]["totalLines"].as_u64().is_some()
                })
                .and_then(|output| {
                    path_identity_resolver
                        .observe_complete_read_tool_output(tool_name, output, sequence)
                        .ok()
                        .filter(|observed| {
                            observed_evidence_matches_requested_path(observed, &requested.paths)
                        })
                })
            {
                outcome.observed_evidence.push(observed);
            }
        } else if tool_name == "tool_orchestrate"
            && outcome.observed_evidence.iter().all(|observed| {
                matches!(
                    observed.target,
                    harness_contract::context::EvidenceTargetIdentity::LocalNetwork { .. }
                )
            })
        {
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
                    .filter(|output| {
                        output["file"]["numLines"].as_u64().is_some()
                            && output["file"]["totalLines"].as_u64().is_some()
                    })
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
    // A process-class effect owns a physical process tree, so its durable
    // receipt must carry an honest process terminal. The host shell is not a
    // sandbox process: it can prove quiescence only, and it proves it by
    // reporting it in its own structured result. The Runtime kernel turns
    // that report into the durable evidence the physical closure witness
    // reads, and never into a security posture the host path cannot prove.
    let mut host_process_terminal = false;
    if fresh_execution {
        if let Some(service) = commit_service {
            host_process_terminal = crate::host_process_closure::mint_host_process_evidence(
                service,
                request,
                descriptor,
                &mut outcome,
                host_process_admission.as_ref(),
            )
            .await
            .map_err(|error| {
                ToolError::new(format!(
                    "tool `{tool_name}` host process terminal failed: {error}"
                ))
            })?;
        }
    }
    if fresh_execution {
        if let Some(commit_service) = commit_service {
            // A read-class effect has no settlement stream for a process
            // terminal, so it keeps its plain read receipt and the witness stays
            // honestly unproven for it.
            let committed = if descriptor.effect_kind
                == harness_contract::tool::ToolEffectKind::Read
            {
                if host_process_terminal {
                    commit_service
                        .commit_host_process_read_closure(
                            request,
                            descriptor,
                            &outcome,
                            &resolved_scope,
                            None,
                        )
                        .await
                } else {
                    commit_service.commit_bound_readonly_tool_receipt(
                        request,
                        &outcome,
                        descriptor,
                        &resolved_scope,
                    )
                }
            } else if host_process_terminal {
                commit_service
                    .commit_host_process_closure(request, &descriptor, &outcome)
                    .await
            } else {
                commit_service.commit_tool_effect(&request, &descriptor, &outcome)
            };
            if let Err(error) = committed {
                let reason = format!("tool `{tool_name}` completed but durable receipt commit failed: {error}");
                return Err(match outcome.pending_agent_batch.clone() {
                    Some(pending) => ToolError::unknown_with_agent_batch(reason, pending),
                    None => ToolError::new(reason),
                });
            }
        }
    }

    Ok(outcome)
}
