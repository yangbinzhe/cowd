#![allow(clippy::doc_markdown, clippy::uninlined_format_args, unused_imports)]
//! Integration tests for cross-module wiring.
//!
//! These tests verify that adjacent modules in the runtime crate actually
//! connect correctly — catching wiring gaps that unit tests miss.

use std::time::Duration;

use runtime::green_contract::{GreenContract, GreenContractOutcome, GreenLevel};
use runtime::{
    apply_policy, BranchFreshness, DiffScope, LaneBlocker, LaneContext, PolicyAction,
    PolicyCondition, PolicyEngine, PolicyRule, ReconcileReason, ReviewStatus, StaleBranchAction,
    StaleBranchPolicy,
};

/// stale_branch + policy_engine integration:
/// When a branch is detected stale, does it correctly flow through
/// PolicyCondition::StaleBranch to generate the expected action?
#[test]
fn stale_branch_detection_flows_into_policy_engine() {
    // given — a stale branch context (2 hours behind main, threshold is 1 hour)
    let stale_context = LaneContext::new(
        "stale-lane",
        GreenLevel::TargetedTests,
        Duration::from_secs(2 * 60 * 60), // 2 hours stale
        LaneBlocker::None,
        ReviewStatus::Pending,
        DiffScope::Full,
        false,
    );

    let engine = PolicyEngine::new(vec![PolicyRule::new(
        "stale-merge-forward",
        PolicyCondition::StaleBranch,
        PolicyAction::MergeForward,
        10,
    )]);

    // when
    let actions = engine.evaluate(&stale_context);

    // then
    assert_eq!(actions, vec![PolicyAction::MergeForward]);
}

/// stale_branch + policy_engine: Fresh branch does NOT trigger stale rules
#[test]
fn fresh_branch_does_not_trigger_stale_policy() {
    let fresh_context = LaneContext::new(
        "fresh-lane",
        GreenLevel::TargetedTests,
        Duration::from_secs(30 * 60), // 30 min stale — under 1 hour threshold
        LaneBlocker::None,
        ReviewStatus::Pending,
        DiffScope::Full,
        false,
    );

    let engine = PolicyEngine::new(vec![PolicyRule::new(
        "stale-merge-forward",
        PolicyCondition::StaleBranch,
        PolicyAction::MergeForward,
        10,
    )]);

    let actions = engine.evaluate(&fresh_context);
    assert!(actions.is_empty());
}

/// green_contract + policy_engine integration:
/// A lane that meets its green contract should be mergeable
#[test]
fn green_contract_satisfied_allows_merge() {
    let contract = GreenContract::new(GreenLevel::Workspace);
    let satisfied = contract.is_satisfied_by(GreenLevel::Workspace);
    assert!(satisfied);

    let exceeded = contract.is_satisfied_by(GreenLevel::MergeReady);
    assert!(exceeded);

    let insufficient = contract.is_satisfied_by(GreenLevel::Package);
    assert!(!insufficient);
}

/// green_contract + policy_engine:
/// Lane with green level below contract requirement gets blocked
#[test]
fn green_contract_unsatisfied_blocks_merge() {
    let context = LaneContext::new(
        "partial-green-lane",
        GreenLevel::Package,
        Duration::from_secs(0),
        LaneBlocker::None,
        ReviewStatus::Pending,
        DiffScope::Full,
        false,
    );

    // LaneContext now uses the GreenLevel enum from green_contract
    let engine = PolicyEngine::new(vec![PolicyRule::new(
        "workspace-green-required",
        PolicyCondition::GreenAt { level: GreenLevel::Workspace },
        PolicyAction::MergeToDev,
        10,
    )]);

    let actions = engine.evaluate(&context);
    assert!(actions.is_empty()); // Package < Workspace, so no merge
}

/// reconciliation + policy_engine integration:
/// A reconciled lane should be handled by reconcile rules, not generic closeout
#[test]
fn reconciled_lane_matches_reconcile_condition() {
    let context = LaneContext::reconciled("reconciled-lane");

    let engine = PolicyEngine::new(vec![
        PolicyRule::new(
            "reconcile-first",
            PolicyCondition::LaneReconciled,
            PolicyAction::Reconcile {
                reason: ReconcileReason::AlreadyMerged,
            },
            5,
        ),
        PolicyRule::new(
            "generic-closeout",
            PolicyCondition::LaneCompleted,
            PolicyAction::CloseoutLane,
            30,
        ),
    ]);

    let actions = engine.evaluate(&context);

    // Both rules fire — reconcile (priority 5) first, then closeout (priority 30)
    assert_eq!(
        actions,
        vec![
            PolicyAction::Reconcile {
                reason: ReconcileReason::AlreadyMerged,
            },
            PolicyAction::CloseoutLane,
        ]
    );
}

/// stale_branch module: apply_policy generates correct actions
#[test]
fn stale_branch_apply_policy_produces_rebase_action() {
    let stale = BranchFreshness::Stale {
        commits_behind: 5,
        missing_fixes: vec!["fix-123".to_string()],
    };

    let action = apply_policy(&stale, StaleBranchPolicy::AutoRebase);
    assert_eq!(action, StaleBranchAction::Rebase);
}

#[test]
fn stale_branch_apply_policy_produces_merge_forward_action() {
    let stale = BranchFreshness::Stale {
        commits_behind: 3,
        missing_fixes: vec![],
    };

    let action = apply_policy(&stale, StaleBranchPolicy::AutoMergeForward);
    assert_eq!(action, StaleBranchAction::MergeForward);
}

#[test]
fn stale_branch_apply_policy_warn_only() {
    let stale = BranchFreshness::Stale {
        commits_behind: 2,
        missing_fixes: vec!["fix-456".to_string()],
    };

    let action = apply_policy(&stale, StaleBranchPolicy::WarnOnly);
    match action {
        StaleBranchAction::Warn { message } => {
            assert!(message.contains("2 commit(s) behind main"));
            assert!(message.contains("fix-456"));
        }
        _ => panic!("expected Warn action, got {:?}", action),
    }
}

#[test]
fn stale_branch_fresh_produces_noop() {
    let fresh = BranchFreshness::Fresh;
    let action = apply_policy(&fresh, StaleBranchPolicy::AutoRebase);
    assert_eq!(action, StaleBranchAction::Noop);
}

/// Combined flow: stale detection + policy + action
#[test]
fn end_to_end_stale_lane_gets_merge_forward_action() {
    // Simulating what a harness would do:
    // 1. Detect branch freshness
    // 2. Build lane context from freshness + other signals
    // 3. Run policy engine
    // 4. Return actions

    // given: detected stale state
    let _freshness = BranchFreshness::Stale {
        commits_behind: 5,
        missing_fixes: vec!["fix-123".to_string()],
    };

    // when: build context and evaluate policy
    let context = LaneContext::new(
        "lane-9411",
        GreenLevel::Workspace,
        Duration::from_secs(5 * 60 * 60), // 5 hours stale, definitely over threshold
        LaneBlocker::None,
        ReviewStatus::Approved,
        DiffScope::Scoped,
        false,
    );

    let engine = PolicyEngine::new(vec![
        // Priority 5: Check if stale first
        PolicyRule::new(
            "auto-merge-forward-if-stale-and-approved",
            PolicyCondition::And(vec![
                PolicyCondition::StaleBranch,
                PolicyCondition::ReviewPassed,
            ]),
            PolicyAction::MergeForward,
            5,
        ),
        // Priority 10: Normal stale handling
        PolicyRule::new(
            "stale-warning",
            PolicyCondition::StaleBranch,
            PolicyAction::Notify {
                channel: "#build-status".to_string(),
            },
            10,
        ),
    ]);

    let actions = engine.evaluate(&context);

    // then: both rules should fire (stale + approved matches both)
    assert_eq!(
        actions,
        vec![
            PolicyAction::MergeForward,
            PolicyAction::Notify {
                channel: "#build-status".to_string(),
            },
        ]
    );
}

/// Fresh branch with approved review should merge (not stale-blocked)
#[test]
fn fresh_approved_lane_gets_merge_action() {
    let context = LaneContext::new(
        "fresh-approved-lane",
        GreenLevel::Workspace,
        Duration::from_secs(30 * 60), // 30 min — under 1 hour threshold = fresh
        LaneBlocker::None,
        ReviewStatus::Approved,
        DiffScope::Scoped,
        false,
    );

    let engine = PolicyEngine::new(vec![PolicyRule::new(
        "merge-if-green-approved-not-stale",
        PolicyCondition::And(vec![
            PolicyCondition::GreenAt { level: GreenLevel::Workspace },
            PolicyCondition::ReviewPassed,
            // NOT PolicyCondition::StaleBranch — fresh lanes bypass this
        ]),
        PolicyAction::MergeToDev,
        5,
    )]);

    let actions = engine.evaluate(&context);
    assert_eq!(actions, vec![PolicyAction::MergeToDev]);
}

/// worker_boot + recovery_recipes + policy_engine integration:
/// When a session completes with a provider failure, does the worker
/// status transition trigger the correct recovery recipe, and does
/// the resulting recovery state feed into policy decisions?
#[test]
fn worker_provider_failure_flows_through_recovery_to_policy() {
    use runtime::recovery_recipes::{
        attempt_recovery, FailureScenario, RecoveryContext, RecoveryResult, RecoveryStep,
    };
    use runtime::worker_boot::{WorkerFailureKind, WorkerRegistry, WorkerStatus};

    // given — a worker that encounters a provider failure during session completion
    let registry = WorkerRegistry::new();
    let worker = registry.create("/tmp/repo-recovery-test", &[], true);

    // Worker reaches ready state
    registry
        .observe(&worker.worker_id, "Ready for your input\n>")
        .expect("ready observe should succeed");
    registry
        .send_prompt(&worker.worker_id, Some("Run analysis"), None)
        .expect("prompt send should succeed");

    // Session completes with provider failure (finish="unknown", tokens=0)
    let failed_worker = registry
        .observe_completion(&worker.worker_id, "unknown", 0)
        .expect("completion observe should succeed");
    assert_eq!(failed_worker.status, WorkerStatus::Failed);
    let failure = failed_worker
        .last_error
        .expect("worker should have recorded error");
    assert_eq!(failure.kind, WorkerFailureKind::Provider);

    // Bridge: WorkerFailureKind -> FailureScenario
    let scenario = FailureScenario::from_worker_failure_kind(failure.kind);
    assert_eq!(scenario, FailureScenario::ProviderFailure);

    // Recovery recipe lookup and execution
    let mut ctx = RecoveryContext::new();
    let result = attempt_recovery(&scenario, &mut ctx);

    // then — recovery should recommend RestartWorker step
    assert!(
        matches!(result, RecoveryResult::Recovered { steps_taken: 1 }),
        "provider failure should recover via single RestartWorker step, got: {result:?}"
    );
    assert!(
        ctx.events().iter().any(|e| {
            matches!(
                e,
                runtime::recovery_recipes::RecoveryEvent::RecoveryAttempted {
                    result: RecoveryResult::Recovered { steps_taken: 1 },
                    ..
                }
            )
        }),
        "recovery should emit structured attempt event"
    );

    // Policy integration: recovery success + green status = merge-ready
    // (Simulating the policy check that would happen after successful recovery)
    let recovery_success = matches!(result, RecoveryResult::Recovered { .. });
    let green_level = GreenLevel::Workspace;
    let not_stale = Duration::from_secs(30 * 60); // 30 min — fresh

    let post_recovery_context = LaneContext::new(
        "recovered-lane",
        green_level,
        not_stale,
        LaneBlocker::None,
        ReviewStatus::Approved,
        DiffScope::Scoped,
        false,
    );

    let policy_engine = PolicyEngine::new(vec![
        // Rule: if recovered from failure + green + approved -> merge
        PolicyRule::new(
            "merge-after-successful-recovery",
            PolicyCondition::And(vec![
                PolicyCondition::GreenAt { level: GreenLevel::Workspace },
                PolicyCondition::ReviewPassed,
            ]),
            PolicyAction::MergeToDev,
            10,
        ),
    ]);

    // Recovery success is a pre-condition; policy evaluates post-recovery context
    assert!(
        recovery_success,
        "recovery must succeed for lane to proceed"
    );
    let actions = policy_engine.evaluate(&post_recovery_context);
    assert_eq!(
        actions,
        vec![PolicyAction::MergeToDev],
        "post-recovery green+approved lane should be merge-ready"
    );
}

/// TeamDiscovery + AgentDirectory integration:
/// TeamDiscoveryProtocol correctly discovers and ranks agents from the
/// global AgentDirectory by skill overlap.
#[test]
fn team_discovery_ranks_by_skill_overlap_from_directory() {
    use runtime::team_discovery::TeamDiscoveryProtocol;
    use memory::agent_directory::{AgentDirectory, AgentInfo, AgentStatus};

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Register agents with varying capabilities
    let agents = [
        AgentInfo {
            agent_id: "rust-expert".into(),
            role: "Executor".into(),
            capabilities: vec!["rust".into(), "testing".into(), "refactoring".into()],
            status: AgentStatus::Active,
            registered_at_ms: now,
            last_heartbeat_ms: now,
            reputation: None,
        },
        AgentInfo {
            agent_id: "python-dev".into(),
            role: "Executor".into(),
            capabilities: vec!["python".into()],
            status: AgentStatus::Active,
            registered_at_ms: now,
            last_heartbeat_ms: now,
            reputation: None,
        },
        AgentInfo {
            agent_id: "tester".into(),
            role: "Reviewer".into(),
            capabilities: vec!["testing".into()],
            status: AgentStatus::Active,
            registered_at_ms: now,
            last_heartbeat_ms: now,
            reputation: None,
        },
    ];

    for a in &agents {
        AgentDirectory::global().register(a.clone());
    }

    let discovery = TeamDiscoveryProtocol::new();
    let ranked = discovery.discover_team(
        "Build a Rust microservice with tests",
        &["rust".into(), "testing".into()],
    );

    assert_eq!(ranked.len(), 2);
    assert_eq!(ranked[0].agent_id, "rust-expert"); // 2 matches (rust+testing)
    assert_eq!(ranked[1].agent_id, "tester");       // 1 match (testing)

    // python-dev doesn't match rust or testing
    assert!(ranked.iter().all(|a| a.agent_id != "python-dev"));

    // Cleanup
    for a in &agents {
        AgentDirectory::global().unregister(&a.agent_id);
    }
}

/// TeamDiscovery + CollaborationOrchestrator integration:
/// The orchestrator's assemble_team() uses reputation-aware discovery
/// and produces a valid AgentTeam with leader and workers.
#[test]
fn orchestrator_assemble_team_uses_discovery_protocol() {
    use memory::agent_directory::{AgentDirectory, AgentInfo, AgentStatus, ReputationScore};
    use runtime::agent_collaboration::{CollaborationOrchestrator, CollaborationTask};
    use runtime::agent::{SubAgentExecutor, SubAgentConfig, SubAgentResult, SubAgentError};

    struct NoopExecutor;
    impl SubAgentExecutor for NoopExecutor {
        fn execute(
            &self,
            _config: SubAgentConfig,
            _task: &str,
        ) -> impl std::future::Future<Output = Result<SubAgentResult, SubAgentError>> + Send {
            async { Ok(SubAgentResult::default()) }
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Register agents with reputation — leader should be the one with highest rep
    let agents = [
        AgentInfo {
            agent_id: "high-rep".into(),
            role: "Executor".into(),
            capabilities: vec!["rust".into()],
            status: AgentStatus::Active,
            registered_at_ms: now,
            last_heartbeat_ms: now,
            reputation: Some(ReputationScore {
                success_rate: 0.95,
                task_count: 50,
                peer_rating: 4.9,
                last_success_at_ms: now,
                recent_failures: 0,
            }),
        },
        AgentInfo {
            agent_id: "low-rep".into(),
            role: "Executor".into(),
            capabilities: vec!["rust".into(), "testing".into()],
            status: AgentStatus::Active,
            registered_at_ms: now,
            last_heartbeat_ms: now,
            reputation: Some(ReputationScore {
                success_rate: 0.2,
                task_count: 3,
                peer_rating: 1.0,
                last_success_at_ms: 0,
                recent_failures: 5,
            }),
        },
    ];

    for a in &agents {
        AgentDirectory::global().register(a.clone());
    }

    let orch = CollaborationOrchestrator::<NoopExecutor>::new(std::sync::Arc::new(NoopExecutor));

    let task = CollaborationTask {
        description: "Rust refactoring".into(),
        required_skills: vec!["rust".into()],
        subtasks: vec![],
        review_criteria: None,
    };

    let team = orch.assemble_team(&task).expect("should assemble team");

    // High-rep agent should be leader despite fewer skill matches
    assert_eq!(team.leader.agent_id, "high-rep");
    assert!(!team.workers.is_empty());
    assert_eq!(team.workers[0].agent_id, "low-rep");

    // Cleanup
    for a in &agents {
        AgentDirectory::global().unregister(&a.agent_id);
    }
}

/// TeamDiscovery auto_assemble + empty directory returns None.
#[test]
fn auto_assemble_returns_none_when_directory_empty() {
    use runtime::team_discovery::TeamDiscoveryProtocol;

    let discovery = TeamDiscoveryProtocol::new();
    let result = discovery.auto_assemble("any task", &["rust".into()]);
    assert!(result.is_none());
}
